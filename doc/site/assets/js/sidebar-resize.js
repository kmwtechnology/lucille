(function () {
  const resizer = document.getElementById('sidebar-resizer');
  const inner = document.querySelector('.td-sidebar__inner');
  if (!resizer || !inner) return;

  const MIN = 150, MAX = 600;
  const saved = localStorage.getItem('sidebarWidth');
  if (saved) document.documentElement.style.setProperty('--sidebar-width', saved + 'px');

  resizer.addEventListener('mousedown', (e) => { });
})();