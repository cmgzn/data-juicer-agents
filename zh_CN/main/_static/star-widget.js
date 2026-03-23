/**
 * Star Widget - GitHub OAuth Star Button
 *
 * Inserts a "Star this repo" button at the top of the documentation index page.
 * Uses GitHub OAuth App flow to let users star the current repo with one click.
 *
 * Flow:
 *   1. User clicks the button
 *   2. Redirect to GitHub OAuth authorization page (scope: public_repo)
 *   3. GitHub redirects to backend /star/callback with code + state
 *   4. Backend exchanges code for access_token, calls PUT /user/starred/{owner}/{repo}
 *   5. Backend redirects back to this page with ?starred=1 or ?star_error=1
 *   6. This script detects the query param and shows a toast notification
 *
 * Required global variables (injected by conf.py via app.add_js_file):
 *   window.GITHUB_OAUTH_CLIENT_ID  - GitHub OAuth App Client ID
 *   window.STAR_REPO_OWNER         - Repository owner (e.g. "datajuicer")
 *   window.STAR_REPO_NAME          - Repository name (e.g. "data-juicer")
 *   window.STAR_API_BASE_URL       - Backend base URL (e.g. "https://datajuicer.online:443")
 */

(function () {
  'use strict';

  // ─── Configuration ────────────────────────────────────────────────────────

  var CLIENT_ID = window.GITHUB_OAUTH_CLIENT_ID || '';
  var REPO_OWNER = window.STAR_REPO_OWNER || '';
  var REPO_NAME = window.STAR_REPO_NAME || '';
  var API_BASE = (window.STAR_API_BASE_URL || '').replace(/\/$/, '');

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Detect current page language from the URL path segment.
   * URL pattern: /{lang}/{version}/pagename.html
   * Returns 'zh_CN' when the path contains /zh_CN/, otherwise 'en'.
   */
  function detectLang() {
    return window.location.pathname.indexOf('/zh_CN/') !== -1 ? 'zh_CN' : 'en';
  }

  var LANG = detectLang();

  var I18N = {
    en: {
      buttonTitle: 'Star ' + REPO_OWNER + '/' + REPO_NAME + ' on GitHub',
      toastSuccess: '🎉 Thanks for starring!',
      toastError: '😕 Something went wrong. Please try again later.',
      toastClose: '×',
    },
    zh_CN: {
      buttonTitle: '在 GitHub 上 Star ' + REPO_OWNER + '/' + REPO_NAME,
      toastSuccess: '🎉 感谢 Star！',
      toastError: '😕 出了点问题，请稍后再试。',
      toastClose: '×',
    },
  };

  var i18n = I18N[LANG] || I18N.en;

  /**
   * Generate a random nonce string for CSRF protection.
   */
  function generateNonce() {
    var arr = new Uint8Array(16);
    window.crypto.getRandomValues(arr);
    return Array.from(arr, function (b) {
      return b.toString(16).padStart(2, '0');
    }).join('');
  }

  /**
   * Build the clean return URL (current page without star-related query params).
   */
  function buildReturnUrl() {
    var url = new URL(window.location.href);
    url.searchParams.delete('starred');
    url.searchParams.delete('star_error');
    url.searchParams.delete('code');
    url.searchParams.delete('state');
    return url.toString();
  }

  /**
   * Construct the GitHub OAuth authorization URL.
   * The `state` param encodes the return URL and a nonce (base64, URL-safe).
   */
  function buildOAuthUrl() {
    var returnUrl = buildReturnUrl();
    var nonce = generateNonce();
    var statePayload = JSON.stringify({ return_url: returnUrl, nonce: nonce, owner: REPO_OWNER, repo: REPO_NAME });
    var state = btoa(statePayload);

    var params = new URLSearchParams({
      client_id: CLIENT_ID,
      scope: 'public_repo',
      state: state,
      redirect_uri: API_BASE + '/star/callback',
    });

    return 'https://github.com/login/oauth/authorize?' + params.toString();
  }

  // ─── Toast Notification ───────────────────────────────────────────────────

  /**
   * Show a toast notification at the bottom-right of the screen.
   * @param {string} message - Message text
   * @param {'success'|'error'} type - Toast type
   */
  function showToast(message, type) {
    // Remove any existing toast
    var existing = document.getElementById('star-widget-toast');
    if (existing) existing.remove();

    var toast = document.createElement('div');
    toast.id = 'star-widget-toast';
    toast.className = 'star-widget-toast star-widget-toast--' + type;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'polite');

    var text = document.createElement('span');
    text.className = 'star-widget-toast__text';
    text.textContent = message;

    var closeBtn = document.createElement('button');
    closeBtn.className = 'star-widget-toast__close';
    closeBtn.setAttribute('aria-label', 'Close notification');
    closeBtn.textContent = i18n.toastClose;
    closeBtn.addEventListener('click', function () {
      toast.classList.add('star-widget-toast--hiding');
      setTimeout(function () { toast.remove(); }, 300);
    });

    toast.appendChild(text);
    toast.appendChild(closeBtn);
    document.body.appendChild(toast);

    // Auto-dismiss after 5 seconds
    setTimeout(function () {
      if (toast.parentNode) {
        toast.classList.add('star-widget-toast--hiding');
        setTimeout(function () { if (toast.parentNode) toast.remove(); }, 300);
      }
    }, 5000);

    // Trigger entrance animation on next frame
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        toast.classList.add('star-widget-toast--visible');
      });
    });
  }

  /**
   * Check URL query params for star result and show appropriate toast.
   * Also cleans up the URL to remove the query params.
   */
  function handleStarResult() {
    var params = new URLSearchParams(window.location.search);
    var starred = params.get('starred');
    var starError = params.get('star_error');

    if (!starred && !starError) return;

    // Clean up URL without reloading
    var cleanUrl = buildReturnUrl();
    window.history.replaceState({}, '', cleanUrl);

    if (starred === '1') {
      showToast(i18n.toastSuccess, 'success');
    } else if (starError === '1') {
      showToast(i18n.toastError, 'error');
    }
  }

  // ─── Navbar Star Button ───────────────────────────────────────────────────

  /**
   * Wire up the navbar Star button rendered by the Jinja template
   * (star-navbar-btn.html). The template emits a static <a href="#">
   * placeholder; we replace href with the real OAuth URL at runtime so that
   * the OAuth state (nonce + return_url) is always fresh.
   */
  function initNavbarStarButton() {
    var navbarBtn = document.getElementById('star-navbar-btn');
    if (!navbarBtn) return;

    navbarBtn.href = buildOAuthUrl();
    navbarBtn.title = i18n.buttonTitle;
    navbarBtn.setAttribute('aria-label', i18n.buttonTitle);
  }

  // ─── Init ─────────────────────────────────────────────────────────────────

  function init() {
    // Guard: only run if configuration is present
    if (!CLIENT_ID || !REPO_OWNER || !REPO_NAME || !API_BASE) {
      return;
    }

    // Handle OAuth callback result (starred=1 / star_error=1)
    handleStarResult();

    // Wire up the navbar button (present on every page via the template)
    initNavbarStarButton();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
