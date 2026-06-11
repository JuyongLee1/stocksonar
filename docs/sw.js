/* STOCKSONAR Service Worker — 푸시 알림 + PWA */
self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));
self.addEventListener('fetch', () => {}); // 네트워크 그대로 통과 (설치성 요건용)

self.addEventListener('push', e => {
  let d = {};
  try { d = e.data.json(); } catch (_) { d = { title: 'STOCKSONAR', body: e.data && e.data.text() }; }
  e.waitUntil(self.registration.showNotification(d.title || '📡 STOCKSONAR', {
    body: d.body || '',
    icon: 'icon-192.png',
    badge: 'icon-192.png',
    tag: d.tag || 'sonar',
    data: { url: d.url || './' }
  }));
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(clients.matchAll({ type: 'window', includeUncontrolled: true }).then(ws => {
    for (const w of ws) { if ('focus' in w) return w.focus(); }
    return clients.openWindow(e.notification.data.url || './');
  }));
});
