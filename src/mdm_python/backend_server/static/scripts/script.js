
function setActiveNavbarItem() {
    
    const navbarItems = document.querySelectorAll('.navbar-nav a');
    const currentUrl = window.location.href;
    navbarItems.forEach(item => {
      if (item.href === currentUrl) {
        item.classList.add('active');
      } else {
        item.classList.remove('active');
      }
    });
  }

