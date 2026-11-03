import http from 'node:http';
import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { Database, ensureDatabaseInitialized, runMigrations, PaymentSessionRepository, ObservationRepository } from '@youpi/db';
import { AttributionEngine, WebhookEngine } from '@youpi/core';
import { createPaymentApp, startServer } from '../index.js';
import { ObserverPollerService } from '../services/observer-poller.service.js';
import { registerCustomerRoutes } from '../routes/customer.routes.js';
import { HttpServer } from '../server.js';

describe('Payment API Lifecycle & Deterministic Asynchronous Shutdown', () => {
  let db;

  beforeEach(() => {
    db = new Database({ inMemory: true });
    ensureDatabaseInitialized(db);
    runMigrations(db);
  });

  afterEach(() => {
    try {
      if (db) {
        db.close();
      }
    } catch {}
  });

  test('1. Shutdown awaits pending startup webhook recovery', async () => {
    let recoveryCompleted = false;
    let resolveRecovery;
    const recoveryDeferred = new Promise(resolve => {
      resolveRecovery = () => {
        recoveryCompleted = true;
        resolve();
      };
    });

    const mockWebhookEngine = new WebhookEngine({
      db,
      merchantRepo: { getPrimaryMerchant: () => ({ id: 'm1', webhookUrl: 'http://localhost:9999', webhookSecret: 'sec' }) }
    });

    // Mock recoverOutbox with artificial delay
    mockWebhookEngine.recoverOutbox = async () => {
      await recoveryDeferred;
      return { recovered: 0 };
    };

    // Instantiate app with custom webhookEngine
    const app = createPaymentApp(db, { webhookEngine: mockWebhookEngine });

    // The startup task was immediately tracked in app!
    assert.equal(recoveryCompleted, false, 'Startup recovery should still be in flight');

    // Initiate destroy while recovery is pending
    let destroyFinished = false;
    const destroyPromise = app.destroy().then(() => {
      destroyFinished = true;
    });

    assert.equal(app.isShuttingDown(), true, 'isShuttingDown should be true');
    assert.equal(destroyFinished, false, 'destroy() must not resolve while recovery is pending');

    // Release recovery after 25ms
    setTimeout(() => {
      resolveRecovery();
    }, 25);

    await destroyPromise;
    assert.equal(recoveryCompleted, true, 'Recovery must be completed before destroy resolves');
    assert.equal(destroyFinished, true, 'destroy() resolves cleanly after recovery finishes');
  });

  test('2. Shutdown awaits in-flight webhook retry execution', async () => {
    const app = createPaymentApp(db);
    let retryCompleted = false;
    let resolveRetry;
    const retryDeferred = new Promise(resolve => {
      resolveRetry = () => {
        retryCompleted = true;
        resolve({ sent: 1, failed: 0 });
      };
    });

    // Simulate an ongoing retry operation tracked in the background task registry
    const inFlightOp = (async () => {
      await retryDeferred;
    })();
    app.trackTask(inFlightOp);

    let destroyFinished = false;
    const destroyPromise = app.destroy().then(() => {
      destroyFinished = true;
    });

    assert.equal(destroyFinished, false, 'destroy() must wait for in-flight tracked tasks');

    // Release retry operation after 25ms
    setTimeout(() => {
      resolveRetry();
    }, 25);

    await destroyPromise;
    assert.equal(retryCompleted, true, 'Tracked webhook retry must be completed');
    assert.equal(destroyFinished, true, 'destroy() resolves after in-flight retry finishes');
  });

  test('3. Shutdown during active observer poller cycle awaits cycle completion and stops attribution', async () => {
    const eventEmitter = new EventEmitter();
    const sessionRepo = new PaymentSessionRepository(db);
    const obsRepo = new ObservationRepository(db);
    const attributionEngine = new AttributionEngine({ sessionRepo, obsRepo, eventEmitter });

    const pollerService = new ObserverPollerService({
      db,
      sessionRepo,
      obsRepo,
      attributionEngine,
      eventEmitter,
      pollIntervalMs: 5000,
      minSweepCooldownMs: 2000,
      autoStart: false
    });

    let cycleCompleted = false;
    let cycleStarted = false;
    let resolveCycle;
    const artificialCycle = new Promise(resolve => {
      resolveCycle = () => {
        cycleCompleted = true;
        resolve();
      };
    });

    // Intercept sweepSmsObserver to simulate delayed in-flight observer sweep
    pollerService.sweepSmsObserver = async () => {
      cycleStarted = true;
      await artificialCycle;
      return true;
    };

    const merchant = db.queryOne("SELECT id FROM merchants LIMIT 1") || { id: 'default_merchant' };
    const account = db.queryOne("SELECT id FROM payment_receiving_accounts WHERE merchant_id = ? AND is_active = 1", [merchant.id]);

    // Insert an active session so pollCycle proceeds
    db.execute(`
      INSERT INTO payment_sessions (id, merchant_id, order_id, amount_paise, status, method, receiving_account_id, created_at, expires_at)
      VALUES ('sess-poller-test', ?, 'order-1', 10000, 'ACTIVE', 'UPI', ?, datetime('now'), datetime('now', '+5 minutes'))
    `, [merchant.id, account.id]);

    pollerService.start();

    // Trigger poll cycle
    const pollPromise = pollerService.pollCycle();
    assert.equal(cycleStarted, true, 'Poll cycle should start');

    // While cycle is active, call stopAsync
    let stopResolved = false;
    const stopPromise = pollerService.stopAsync().then(() => {
      stopResolved = true;
    });

    assert.equal(pollerService.isRunning, false, 'isRunning should immediately be set to false');
    assert.equal(stopResolved, false, 'stopAsync should not resolve while cycle is active');

    // Release cycle after 20ms
    setTimeout(() => {
      resolveCycle();
    }, 20);

    await stopPromise;
    await pollPromise;

    assert.equal(cycleCompleted, true, 'Active poll cycle completed');
    assert.equal(stopResolved, true, 'stopAsync resolved after cycle completed');
    assert.equal(pollerService.isPollingCycle, false);
    assert.equal(pollerService.isAttributionInProgress, false);
  });

  test('4. Customer route cleanup timer disposal', async () => {
    const server = new HttpServer();
    const context = {
      customerRepo: {},
      customerSessionRepo: {},
      sessionRepo: {},
      auditRepo: {}
    };

    const cleanup = registerCustomerRoutes(server, context);
    assert.equal(typeof cleanup, 'function', 'registerCustomerRoutes must return a cleanup function');

    // Calling cleanup must execute cleanly and be safe to invoke multiple times
    cleanup();
    cleanup();
  });

  test('5. Multiple concurrent and repeated destroy() and close() calls are safe, idempotent, and return the same promise', async () => {
    const serverInstance = await startServer(0, db);

    let callbackError = 'not-called';
    let callbackCalled = false;
    const callbackPromise = new Promise(resolve => {
      serverInstance.close((err) => {
        callbackError = err;
        callbackCalled = true;
        resolve();
      });
    });

    // Call close() multiple times concurrently
    const c1 = serverInstance.close();
    const c2 = serverInstance.close();
    const c3 = serverInstance.close();

    assert.equal(c1, c2, 'Repeated close calls return the exact same close promise');
    assert.equal(c2, c3, 'Repeated close calls return the exact same close promise');

    await Promise.all([c1, c2, c3, callbackPromise]);
    assert.equal(callbackCalled, true, 'close(cb) must invoke the callback');
    assert.equal(callbackError, null, 'close(cb) must invoke callback with null on success');

    // Multiple app.destroy() calls
    const [d1, d2] = await Promise.all([
      serverInstance.app.destroy(),
      serverInstance.app.destroy()
    ]);

    assert.equal(serverInstance.app.isShuttingDown(), true);
  });

  test('6. No database operations occur after shutdown resolves and database can be closed safely', async () => {
    const serverInstance = await startServer(0, db);
    const merchant = db.queryOne("SELECT id FROM merchants LIMIT 1") || { id: 'default_merchant' };
    const account = db.queryOne("SELECT id FROM payment_receiving_accounts WHERE merchant_id = ? AND is_active = 1", [merchant.id]);

    // Enqueue an active session
    db.execute(`
      INSERT INTO payment_sessions (id, merchant_id, order_id, amount_paise, status, method, receiving_account_id, created_at, expires_at)
      VALUES ('sess-safe-test', ?, 'order-safe', 10000, 'ACTIVE', 'UPI', ?, datetime('now'), datetime('now', '+5 minutes'))
    `, [merchant.id, account.id]);

    // Await complete shutdown
    await serverInstance.close();

    // The database must now be safe to close immediately without lingering queries throwing SQLITE_BUSY or closed DB error
    assert.doesNotThrow(() => {
      db.close();
    }, 'Closing SQLite database after server.close() must not throw');

    // Wait 150ms to ensure no unhandled asynchronous background timers try to query the closed database
    await new Promise(resolve => setTimeout(resolve, 150));
  });

  test('7. Client disconnects while an asynchronous route handler is still accessing SQLite', async () => {
    const serverInstance = await startServer(0, db);
    let handlerStarted = false;
    let handlerFinished = false;
    let resolveHandler;
    const handlerDeferred = new Promise(resolve => { resolveHandler = resolve; });

    serverInstance.app.server.get('/test-async-disconnect', async (req, res) => {
      handlerStarted = true;
      await handlerDeferred;
      const row = db.queryOne('SELECT COUNT(*) as count FROM merchants');
      handlerFinished = true;
      if (!res.destroyed && !res.writableEnded) {
        HttpServer.json(res, 200, { ok: true, count: row.count });
      }
    });

    const req = http.get(`http://127.0.0.1:${serverInstance.port}/test-async-disconnect`);
    req.on('error', () => {});

    while (!handlerStarted) {
      await new Promise(r => setTimeout(r, 5));
    }

    req.destroy();

    let closeResolved = false;
    const closePromise = serverInstance.close().then(() => {
      closeResolved = true;
    });

    assert.equal(closeResolved, false, 'close() must not resolve even if client socket disconnected');
    assert.equal(handlerFinished, false);

    resolveHandler();
    await closePromise;

    assert.equal(handlerFinished, true, 'Handler must finish executing SQLite query before close resolves');
    assert.equal(closeResolved, true);
  });

  test('8. HTTP drain timeout expires while request remains active; shutdown awaits complete handler settlement', async () => {
    const serverInstance = await startServer(0, db);
    let handlerFinished = false;
    let resolveHandler;
    const handlerDeferred = new Promise(resolve => { resolveHandler = resolve; });

    serverInstance.app.server.get('/test-long-running', async (req, res) => {
      await handlerDeferred;
      const count = db.queryOne('SELECT COUNT(*) as c FROM merchants');
      handlerFinished = true;
      HttpServer.json(res, 200, { count: count.c });
    });

    const clientReq = http.get(`http://127.0.0.1:${serverInstance.port}/test-long-running`);
    clientReq.on('error', () => {});

    await new Promise(r => setTimeout(r, 20));

    let drainResolved = false;
    const drainPromise = serverInstance.app.server.drainRequests(10).then(() => {
      drainResolved = true;
    });

    await new Promise(r => setTimeout(r, 30));

    assert.equal(drainResolved, false, 'drainRequests() must not resolve prematurely when grace timeout expires');
    assert.equal(handlerFinished, false);

    resolveHandler();
    await drainPromise;

    assert.equal(handlerFinished, true, 'Handler must settle completely');
    assert.equal(drainResolved, true, 'drainRequests() resolves only after handler completes');
    await serverInstance.close();
  });

  test('9. New HTTP requests receive 503 after shutdown begins', async () => {
    const serverInstance = await startServer(0, db);

    serverInstance.app.server.isClosing = true;

    const res = await new Promise((resolve, reject) => {
      const req = http.get(`http://127.0.0.1:${serverInstance.port}/health`, res => {
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('end', () => resolve({ statusCode: res.statusCode, body }));
      });
      req.on('error', reject);
    });

    assert.equal(res.statusCode, 503, 'New requests must receive 503 Service Unavailable');
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.error, 'Server is shutting down');

    await serverInstance.close();
  });

  test('10. SSE connections close cleanly on shutdown', async () => {
    const serverInstance = await startServer(0, db);
    let sseConnected = false;
    let shutdownEventReceived = false;

    // Direct endpoint using initSse
    serverInstance.app.server.get('/test-sse-events', (req, res) => {
      serverInstance.app.server.initSse('test-topic', res);
    });

    let resolveEnded;
    const clientEndedPromise = new Promise(resolve => { resolveEnded = resolve; });

    const req = http.get(`http://127.0.0.1:${serverInstance.port}/test-sse-events`, res => {
      res.on('data', chunk => {
        const str = chunk.toString();
        if (str.includes('event: connected')) {
          sseConnected = true;
        }
        if (str.includes('event: shutdown')) {
          shutdownEventReceived = true;
        }
      });
      res.on('end', resolveEnded);
      res.on('close', resolveEnded);
    });

    // Wait until SSE connects
    while (!sseConnected) {
      await new Promise(r => setTimeout(r, 5));
    }

    assert.equal(sseConnected, true, 'SSE client must connect');

    await serverInstance.close();
    await clientEndedPromise;

    assert.equal(shutdownEventReceived, true, 'SSE client must receive shutdown event');
  });

  test('11. Two webhook retry timer ticks cannot execute overlapping sweeps; shutdown waits for active sweep', async () => {
    let recoverCount = 0;
    let resolveRecovery;
    const recoveryDeferred = new Promise(resolve => {
      resolveRecovery = () => {
        resolve();
      };
    });

    const mockWebhookEngine = new WebhookEngine({
      db,
      merchantRepo: { getPrimaryMerchant: () => ({ id: 'm1', webhookUrl: 'http://localhost:9999', webhookSecret: 'sec' }) }
    });

    mockWebhookEngine.recoverOutbox = async () => {
      recoverCount++;
      await recoveryDeferred;
      return { recovered: 0 };
    };
    mockWebhookEngine.retryPending = async () => ({ sent: 0, failed: 0 });

    const app = createPaymentApp(db, { webhookEngine: mockWebhookEngine });

    // Tick 1 is started automatically during startup or call executeWebhookSweep
    const sweep1Promise = app.executeWebhookSweep();

    // Tick 2 is fired while sweep 1 is still in flight
    const sweep2Promise = app.executeWebhookSweep();

    // Invariant: recoverOutbox was called ONLY once!
    assert.equal(recoverCount, 1, 'Concurrent sweep invocation must not trigger overlapping recoverOutbox()');
    assert.equal(sweep1Promise, sweep2Promise, 'Second sweep invocation must share the existing in-flight sweep promise');

    // Shutdown must wait for active sweep
    let shutdownDone = false;
    const destroyPromise = app.destroy().then(() => {
      shutdownDone = true;
    });

    assert.equal(shutdownDone, false, 'destroy() must wait for active sweep');

    // Release recovery
    resolveRecovery();
    await sweep1Promise;
    await destroyPromise;

    assert.equal(shutdownDone, true, 'destroy() resolves after in-flight sweep settles');
  });

  test('12. server.close() awaits the Node HTTP server close callback before completing destruction', async () => {
    const serverInstance = await startServer(0, db);
    const rawServer = serverInstance.app.rawServer;
    const originalClose = rawServer.close.bind(rawServer);

    let rawCloseCallbackInvoked = false;
    let destroyStarted = false;
    const origDestroy = serverInstance.app.destroy.bind(serverInstance.app);
    serverInstance.app.destroy = async () => {
      destroyStarted = true;
      assert.equal(rawCloseCallbackInvoked, true, 'app.destroy() must be called ONLY AFTER the Node HTTP server close callback fires');
      return origDestroy();
    };

    rawServer.close = function (cb) {
      return originalClose((err) => {
        // Artificially delay calling cb to verify that serverInstance.close awaits the callback
        setTimeout(() => {
          rawCloseCallbackInvoked = true;
          if (cb) cb(err);
        }, 40);
      });
    };

    let closeFinished = false;
    const closePromise = serverInstance.close().then(() => {
      closeFinished = true;
    });

    assert.equal(rawCloseCallbackInvoked, false);
    assert.equal(destroyStarted, false);
    assert.equal(closeFinished, false, 'close() must not resolve before Node HTTP server close callback fires');

    await closePromise;
    assert.equal(rawCloseCallbackInvoked, true, 'Node HTTP server close callback must have completed');
    assert.equal(destroyStarted, true, 'app.destroy() completed');
    assert.equal(closeFinished, true);
  });

  test('13. server.close(cb) propagates callback error when instance.close fails', async () => {
    const serverInstance = await startServer(0, db);
    const rawServer = serverInstance.app.rawServer;
    const originalClose = rawServer.close.bind(rawServer);

    const testError = new Error('SIMULATED_SERVER_CLOSE_ERROR');
    rawServer.close = function (cb) {
      return originalClose(() => {
        if (cb) cb(testError);
      });
    };

    let receivedErr = null;
    const cbPromise = new Promise(resolve => {
      serverInstance.close((err) => {
        receivedErr = err;
        resolve();
      });
    });

    await assert.rejects(serverInstance.close(), /SIMULATED_SERVER_CLOSE_ERROR/);
    await cbPromise;
    assert.equal(receivedErr, testError, 'Callback must receive the simulated error');
  });
});
