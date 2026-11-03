import fs from 'node:fs';
import { EventEmitter } from 'node:events';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PaymentStatus } from '@youpi/common';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, '../../..');

// In production, reliably ensure environment variables from .env are loaded if not already provided
if (!process.env.BASE_CHECKOUT_URL && typeof process.loadEnvFile === 'function') {
  try {
    const rootEnv = path.resolve(rootDir, '.env');
    if (fs.existsSync(rootEnv)) {
      process.loadEnvFile(rootEnv);
    }
  } catch {}
}
import {
  getDatabase,
  ensureDatabaseInitialized,
  runMigrations,
  MerchantRepository,
  ReceivingAccountRepository,
  CustomerRepository,
  CustomerSessionRepository,
  PaymentSessionRepository,
  ObservationRepository,
  SupportTicketRepository,
  AuditLogRepository,
  DeviceRepository
} from '@youpi/db';
import {
  QueueEngine,
  TimingEngine,
  AttributionEngine,
  RecoveryEngine,
  WebhookEngine,
  PaymentVerificationPipeline
} from '@youpi/core';
import { HttpServer } from './server.js';
import { authGuard } from './auth-guard.js';
import { registerPaymentRoutes } from './routes/payments.routes.js';
import { registerCheckoutRoutes } from './routes/checkout.routes.js';
import { registerPollerRoutes } from './routes/pollers.routes.js';
import { registerAdminRoutes } from './routes/admin.routes.js';
import { registerStatementRoutes } from './routes/statement.routes.js';
import { registerCustomerRoutes } from './routes/customer.routes.js';
import { registerDeviceRoutes } from './routes/device.routes.js';
import { getWhatsAppBerryService, WhatsAppBerryService } from './services/whatsapp-berry.service.js';
import { ObserverPollerService } from './services/observer-poller.service.js';

export function createPaymentApp(customDb = null, options = {}) {
  const db = customDb || getDatabase();
  ensureDatabaseInitialized(db);
  const { defaultMerchant } = runMigrations(db);
  const isCustom = !!customDb;
  const whatsappBerryService = options.whatsappBerryService || (isCustom ? new WhatsAppBerryService({
    sessionId: 'test_berry_session',
    sessionPath: path.resolve(rootDir, 'data/test_berry_auth'),
    databasePath: path.resolve(rootDir, 'data/test_berry_store.db'),
    metaFile: path.resolve(rootDir, 'data/test_whatsapp_session.json')
  }) : getWhatsAppBerryService());

  const merchantRepo = new MerchantRepository(db);
  const accountRepo = new ReceivingAccountRepository(db);
  const customerRepo = new CustomerRepository(db);
  const customerSessionRepo = new CustomerSessionRepository(db, customerRepo);
  const sessionRepo = new PaymentSessionRepository(db);
  const obsRepo = new ObservationRepository(db);
  const ticketRepo = new SupportTicketRepository(db);
  const auditRepo = new AuditLogRepository(db);
  const deviceRepo = new DeviceRepository(db);

  const eventEmitter = new EventEmitter();

  // In-flight background task registry and shutdown lifecycle state
  let isShuttingDown = false;
  const inFlightTasks = new Set();

  function trackTask(promise) {
    if (!promise || typeof promise.then !== 'function') {
      return promise;
    }
    const tracked = promise.then(
      () => {},
      () => {}
    ).finally(() => {
      inFlightTasks.delete(tracked);
    });
    inFlightTasks.add(tracked);
    return promise;
  }

  async function drainTasks() {
    while (inFlightTasks.size > 0) {
      const currentTasks = Array.from(inFlightTasks);
      await Promise.allSettled(currentTasks);
    }
  }

  // The active window and the processing window drive the slot expiry that is written to
  // the database, so a malformed value must not reach it: a negative value would expire
  // every session instantly and Infinity would throw a RangeError on toISOString().
  const parseWindowMs = (raw, fallback, label) => {
    if (raw === undefined || raw === null || String(raw).trim() === '') return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0 || value > 24 * 60 * 60 * 1000) {
      console.warn(`[config] Ignoring invalid ${label}=${raw}; using ${fallback}ms.`);
      return fallback;
    }
    return Math.floor(value);
  };
  const activeDurationMs = parseWindowMs(process.env.PAYMENT_ACTIVE_WINDOW_MS, 300000, 'PAYMENT_ACTIVE_WINDOW_MS');
  const processingDurationMs = parseWindowMs(process.env.PAYMENT_PROCESSING_WINDOW_MS, 30000, 'PAYMENT_PROCESSING_WINDOW_MS');

  const queueEngine = new QueueEngine({
    sessionRepo,
    accountRepo,
    eventEmitter,
    auditRepo,
    activeDurationMs,
    processingDurationMs
  });

  const timingEngine = new TimingEngine({
    sessionRepo,
    queueEngine,
    eventEmitter,
    auditRepo,
    activeDurationMs,
    processingDurationMs
  });

  const attributionEngine = new AttributionEngine({
    sessionRepo,
    obsRepo,
    ticketRepo,
    auditRepo,
    eventEmitter
  });

  const webhookEngine = options.webhookEngine || new WebhookEngine({
    db,
    merchantRepo,
    maxRetries: Number(process.env.WEBHOOK_MAX_RETRIES || 5)
  });

  const recoveryEngine = new RecoveryEngine({
    db,
    sessionRepo,
    accountRepo,
    queueEngine,
    timingEngine,
    webhookEngine,
    auditRepo,
    eventEmitter
  });

  const server = new HttpServer();
  server.use(authGuard);
  let checkoutBaseUrl = process.env.CHECKOUT_PUBLIC_URL
    || process.env.PUBLIC_APP_URL
    || (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : null)
    || process.env.BASE_CHECKOUT_URL
    || 'http://localhost:7000';
  try {
    const rootEnv = path.resolve(__dirname, '../../../.env');
    if (fs.existsSync(rootEnv)) {
      const raw = fs.readFileSync(rootEnv, 'utf8');
      for (const line of raw.split(/\r?\n/)) {
        const trimmed = line.trim();
        if (trimmed.startsWith('BASE_CHECKOUT_URL=') && !process.env.CHECKOUT_PUBLIC_URL && !process.env.PUBLIC_APP_URL) {
          const val = trimmed.split('=')[1]?.trim();
          if (val) checkoutBaseUrl = val.replace(/^["']|["']$/g, '');
        }
      }
    }
  } catch {}

  // BULLETPROOF QUEUE PROMOTION HANDLER
  function handleSlotFreedAndPromote(receivingAccountId) {
    if (isShuttingDown) return null;
    if (!receivingAccountId) return null;
    const promoted = queueEngine.promoteNext(receivingAccountId);
    if (promoted) {
      if (promoted.expiresAt) {
        timingEngine.startActiveTimer(promoted.id, promoted.expiresAt);
      }
      server.broadcastSse(promoted.id, 'status_change', {
        status: PaymentStatus.ACTIVE,
        queuePosition: 0,
        expiresAt: promoted.expiresAt
      });
      server.broadcastSse(`queue_${receivingAccountId}`, 'queue_update', {
        promotedId: promoted.id
      });
    }
    return promoted;
  }

  eventEmitter.on('PAYMENT.OBSERVED_SUCCESS', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.OBSERVED_SUCCESS,
        settlementState: 'NOT_RECONCILED',
        verificationLevel: data.verificationLevel || session.verificationLevel || 1,
        verificationTag: data.verificationTag || session.verificationTag,
        sources: data.sources || session.verificationSources
      });
      trackTask(webhookEngine.dispatch('OBSERVED_SUCCESS', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.MANUAL_REVIEW', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.MANUAL_REVIEW,
        settlementState: 'MANUAL_REVIEW',
        reason: data.reason || null
      });
      trackTask(webhookEngine.dispatch('MANUAL_REVIEW', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.RECONCILED', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session && session.status === PaymentStatus.RECONCILED) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.RECONCILED,
        settlementState: 'RECONCILED',
        statementLineId: data.statementLineId || null
      });
      trackTask(webhookEngine.dispatch('RECONCILED', session).catch(() => {}));
    }
  });

  // When a payment succeeds, fails, expires, or cancels: dispatch webhook & promote next queued customer
  eventEmitter.on('PAYMENT.SUCCESS', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.SUCCESS,
        verificationLevel: data.verificationLevel || session.verificationLevel || 1,
        verificationTag: data.verificationTag || session.verificationTag,
        sources: data.sources || session.verificationSources
      });
      trackTask(webhookEngine.dispatch('SUCCESS', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.LEVEL1_VERIFIED', (data) => {
    if (isShuttingDown) return;
    server.broadcastSse(data.paymentId, 'status_change', {
      status: 'VERIFYING',
      verificationLevel: 1,
      verificationTag: data.verificationTag,
      message: 'Payment detected! Verifying secondary channel...'
    });
  });

  eventEmitter.on('PAYMENT.FAILED', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      trackTask(webhookEngine.dispatch('FAILED', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.EXPIRED', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.EXPIRED
      });
      trackTask(webhookEngine.dispatch('EXPIRED', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.CANCELLED', (data) => {
    if (isShuttingDown) return;
    const session = sessionRepo.findById(data.paymentId);
    if (session) {
      server.broadcastSse(session.id, 'status_change', {
        status: PaymentStatus.CANCELLED
      });
      trackTask(webhookEngine.dispatch('CANCELLED', session).catch(() => {}));
      handleSlotFreedAndPromote(session.receivingAccountId);
    }
  });

  eventEmitter.on('PAYMENT.FRAUD_BLOCKED', (data) => {
    if (isShuttingDown) return;
    if (data.sessionId) {
      server.broadcastSse(data.sessionId, 'status_change', {
        status: 'BLOCKED',
        isBanned: true,
        fraudDetected: true,
        orderId: data.orderId,
        reason: data.reason || 'FRAUDULENT_PAYMENT_INJECTION',
        blockedNotice: {
          title: 'FRAUDULENT ACTIVITY DETECTED',
          subtitle: 'Payment Session Terminated & Account Suspended',
          message: 'Our automated verification engine detected fraudulent payment activity associated with this account. Access has been immediately suspended while our security operations team investigates the issue.',
          investigating: true
        }
      });
      server.broadcastSse(data.sessionId, 'account_blocked', {
        status: 'BLOCKED',
        isBanned: true,
        fraudDetected: true,
        orderId: data.orderId,
        reason: data.reason || 'FRAUDULENT_PAYMENT_INJECTION'
      });
    }
  });


  // Continuous 1-second background queue sync and auto-expiry daemon
  const queueSyncDaemon = setInterval(() => {
    if (isShuttingDown) return;
    try {
      const nowIso = new Date().toISOString();

      // Sweep expired ACTIVE sessions
      const expiredActive = db.query(`
        SELECT id, merchant_id, order_id, receiving_account_id FROM payment_sessions
        WHERE status = 'ACTIVE' AND expires_at <= ?
      `, [nowIso]);

      for (const row of expiredActive) {
        if (isShuttingDown) break;
        sessionRepo.transitionToTerminal(row.id, PaymentStatus.EXPIRED);
        server.broadcastSse(row.id, 'status_change', { status: PaymentStatus.EXPIRED });
        eventEmitter.emit('PAYMENT.EXPIRED', {
          paymentId: row.id,
          orderId: row.order_id,
          merchantId: row.merchant_id
        });
        handleSlotFreedAndPromote(row.receiving_account_id);
      }

      // Sweep failed PROCESSING sessions
      const failedProc = db.query(`
        SELECT id, merchant_id, order_id, receiving_account_id FROM payment_sessions
        WHERE status = 'PROCESSING' AND processing_expires_at <= ?
      `, [nowIso]);

      for (const row of failedProc) {
        if (isShuttingDown) break;
        sessionRepo.transitionToTerminal(row.id, PaymentStatus.FAILED);
        server.broadcastSse(row.id, 'status_change', { status: PaymentStatus.FAILED });
        eventEmitter.emit('PAYMENT.FAILED', {
          paymentId: row.id,
          orderId: row.order_id,
          merchantId: row.merchant_id
        });
        handleSlotFreedAndPromote(row.receiving_account_id);
      }

      // Scan for vacant receiving accounts that have waiting queued sessions
      const vacantAccounts = db.query(`
        SELECT id FROM payment_receiving_accounts
        WHERE is_active = 1 AND (is_slot_busy = 0 OR current_active_session_id IS NULL)
      `);

      for (const acc of vacantAccounts) {
        if (isShuttingDown) break;
        handleSlotFreedAndPromote(acc.id);
      }
    } catch {}
  }, 1000);

  if (queueSyncDaemon.unref) queueSyncDaemon.unref();

  // Shared in-flight promise ensuring at most one webhook sweep executes at any given time
  let activeWebhookSweepPromise = null;

  function executeWebhookSweep() {
    if (isShuttingDown) return null;
    if (activeWebhookSweepPromise) {
      return activeWebhookSweepPromise;
    }

    let sweepPromise;
    sweepPromise = (async () => {
      try {
        await webhookEngine.recoverOutbox();
        if (isShuttingDown) return;
        await webhookEngine.retryPending();
      } catch (err) {
        // Silently absorb sweep errors to avoid unhandled rejection in background daemon
      } finally {
        if (activeWebhookSweepPromise === sweepPromise) {
          activeWebhookSweepPromise = null;
        }
      }
    })();

    activeWebhookSweepPromise = sweepPromise;
    trackTask(sweepPromise);
    return sweepPromise;
  }

  // Continuous Recurring Webhook Retry Daemon (Sweeps and dispatches pending webhook deliveries every 5s)
  const webhookRetryDaemon = setInterval(() => {
    if (isShuttingDown) return;
    executeWebhookSweep().catch(() => {});
  }, Number(process.env.WEBHOOK_RETRY_INTERVAL_MS || 5000));

  if (webhookRetryDaemon.unref) webhookRetryDaemon.unref();

  // Consolidated, Coordinated Observer Polling & Verification Daemon
  // Eliminates duplicate HTTP sweeps, enforces 1.5s-2s cooldown, and prevents SQLite write contention
  const observerPollerService = options.observerPollerService || new ObserverPollerService({
    db,
    sessionRepo,
    obsRepo,
    attributionEngine,
    eventEmitter,
    pollIntervalMs: Number(process.env.OBSERVER_POLL_INTERVAL_MS || 1500),
    minSweepCooldownMs: Number(process.env.OBSERVER_SWEEP_COOLDOWN_MS || 2000),
    smsPort: Number(process.env.SMS_OBSERVER_PORT || 6002),
    gmailPort: Number(process.env.GMAIL_OBSERVER_PORT || 6003),
    smsBaseUrl: process.env.SMS_OBSERVER_URL,
    gmailBaseUrl: process.env.GMAIL_OBSERVER_URL
  });

  // Execute crash recovery on boot (AFTER event listeners are wired so recovered terminal transitions emit webhooks)
  const recoveryStats = recoveryEngine.recover();

  // Replay only outboxed transitions that lack a durable delivery record, then run initial retry sweep
  const startupWebhookPromise = (async () => {
    try {
      await executeWebhookSweep();
    } catch (err) {
      console.error('[webhook] Failed to recover terminal-event outbox:', err?.message || err);
    }
  })();
  trackTask(startupWebhookPromise);

  // Health check routes
  const healthHandler = (req, res) => {
    HttpServer.json(res, 200, {
      status: 'UP',
      service: 'payment-api',
      timestamp: new Date().toISOString(),
      recoveryStats
    });
  };
  server.get('/health', healthHandler);
  server.get('/api/v1/health', healthHandler);

  const pipeline = new PaymentVerificationPipeline({
    db,
    sessionRepo,
    obsRepo,
    customerRepo,
    attributionEngine,
    auditRepo,
    merchantRepo,
    server,
    eventEmitter
  });

  const context = {
    db,
    merchantRepo,
    accountRepo,
    customerRepo,
    customerSessionRepo,
    sessionRepo,
    deviceRepo,
    obsRepo,
    ticketRepo,
    auditRepo,
    queueEngine,
    timingEngine,
    attributionEngine,
    pipeline,
    recoveryEngine,
    eventEmitter,
    defaultMerchant,
    checkoutBaseUrl,
    whatsappBerryService,
    webhookEngine,
    observerPollerService,
    otpCooldownMs: options.otpCooldownMs
  };

  registerPaymentRoutes(server, context);
  registerCheckoutRoutes(server, context);
  registerPollerRoutes(server, context);
  registerAdminRoutes(server, context);
  registerStatementRoutes(server, context);
  const cleanupCustomerRoutes = registerCustomerRoutes(server, context);
  registerDeviceRoutes(server, context);

  let destroyPromise = null;

  function destroy() {
    if (destroyPromise) return destroyPromise;

    isShuttingDown = true;

    // 1. Synchronously stop recurring timers and synchronous resources
    clearInterval(queueSyncDaemon);
    clearInterval(webhookRetryDaemon);

    if (typeof cleanupCustomerRoutes === 'function') {
      try {
        cleanupCustomerRoutes();
      } catch (err) {
        console.error('[shutdown] Error in customer route cleanup:', err);
      }
    }

    try {
      timingEngine.destroy();
    } catch (err) {
      console.error('[shutdown] Error destroying timing engine:', err);
    }

    if (typeof attributionEngine.destroy === 'function') {
      try {
        attributionEngine.destroy();
      } catch (err) {
        console.error('[shutdown] Error destroying attribution engine:', err);
      }
    }

    // 2. Coordinate asynchronous cleanup
    destroyPromise = (async () => {
      // Drain in-flight HTTP requests and terminate SSE streams
      if (server && typeof server.drainRequests === 'function') {
        try {
          await server.drainRequests();
        } catch (err) {
          console.error('[shutdown] Error draining HTTP server requests:', err);
        }
      }

      // Await observer poller shutdown (waits for active pollCycle)
      try {
        if (observerPollerService && typeof observerPollerService.stopAsync === 'function') {
          await observerPollerService.stopAsync();
        } else if (observerPollerService && typeof observerPollerService.destroy === 'function') {
          observerPollerService.destroy();
        }
      } catch (err) {
        console.error('[shutdown] Error stopping observer poller service:', err);
      }

      // Await tracked background tasks (webhook recovery, retries, event dispatches)
      try {
        await drainTasks();
      } catch (err) {
        console.error('[shutdown] Error draining background tasks:', err);
      }

      // Await WhatsApp service disconnect if available
      if (whatsappBerryService && typeof whatsappBerryService.disconnect === 'function') {
        try {
          await whatsappBerryService.disconnect({ cleanSession: false });
        } catch (err) {
          // Keep safe
        }
      }
    })();

    return destroyPromise;
  }

  return {
    server,
    context,
    destroy,
    rawServer: server.createServer(),
    isShuttingDown: () => isShuttingDown,
    inFlightTasksCount: () => inFlightTasks.size,
    trackTask,
    drainTasks,
    executeWebhookSweep
  };
}

export function startServer(port = null, customDb = null) {
  const finalPort = port ?? Number(
    process.env.PORT || process.env.PAYMENT_API_PORT || 6100
  );
  const app = createPaymentApp(customDb);

  return new Promise(resolve => {
    // Loopback by default; reach this service via the local Cloudflare Tunnel, not 0.0.0.0.
    const bindHost = process.env.YOUPI_BIND_HOST || (process.env.RENDER ? '0.0.0.0' : '127.0.0.1');
    const instance = app.rawServer.listen(finalPort, bindHost, () => {
      const boundPort = instance.address()?.port || finalPort;
      console.log(`[Payment API] Server listening on port ${boundPort}`);
      let closePromise = null;
      const close = (cb) => {
        if (!closePromise) {
          closePromise = (async () => {
            // 1. Mark HttpServer as closing so any newly arriving requests receive 503
            if (app.server) {
              app.server.isClosing = true;
            }

            // 2. Stop Node HTTP server from accepting new TCP connections
            let resolveServerClose;
            let rejectServerClose;
            const serverClosePromise = new Promise((resolve, reject) => {
              resolveServerClose = resolve;
              rejectServerClose = reject;
            });
            // Guard against unhandled rejections if an error occurs while subsequent steps execute
            serverClosePromise.catch(() => {});

            try {
              instance.close((err) => {
                if (err) {
                  rejectServerClose(err);
                } else {
                  resolveServerClose();
                }
              });
            } catch (err) {
              rejectServerClose(err);
            }

            // 3. Drain active HTTP requests and gracefully close SSE connections
            if (app.server && typeof app.server.drainRequests === 'function') {
              try {
                await app.server.drainRequests();
              } catch (err) {
                console.error('[shutdown] Error draining HTTP server requests:', err);
              }
            }

            // 4. Close any remaining idle keep-alive connections
            if (typeof instance.closeIdleConnections === 'function') {
              try {
                instance.closeIdleConnections();
              } catch (err) {
                console.error('[shutdown] Error closing idle connections:', err);
              }
            }

            // 5. Await complete Node HTTP server closure
            let closeError = null;
            try {
              await serverClosePromise;
            } catch (err) {
              if (err?.code !== 'ERR_SERVER_NOT_RUNNING') {
                console.error('[shutdown] Error closing HTTP server instance:', err);
                closeError = err;
              }
            }

            // 6. Destroy app (stops timers, awaits observer poller, drains webhook tasks, disconnects WhatsApp)
            try {
              await app.destroy();
            } catch (err) {
              console.error('[shutdown] Error destroying app during close:', err);
              if (!closeError) closeError = err;
            }

            if (closeError) {
              throw closeError;
            }
          })();
        }

        if (typeof cb === 'function') {
          closePromise.then(() => cb(null), (err) => cb(err));
        }
        return closePromise;
      };

      resolve({
        app,
        port: boundPort,
        close
      });
    });
  });
}

// Auto-run if executed directly as main script
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  startServer();
}
