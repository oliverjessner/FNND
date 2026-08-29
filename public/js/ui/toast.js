import { dom } from './dom.js';
import { clear } from '../utils/dom.js';

let timer = null;

export function showToast(message, { type = 'info', actionLabel = '', onAction = null } = {}) {
    if (!dom.toastRegion) return;
    if (timer) clearTimeout(timer);
    clear(dom.toastRegion);
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    const label = document.createElement('span');
    label.textContent = message;
    toast.appendChild(label);
    if (actionLabel && typeof onAction === 'function') {
        const action = document.createElement('button');
        action.type = 'button';
        action.textContent = actionLabel;
        action.addEventListener('click', async () => {
            action.disabled = true;
            try { await onAction(); toast.remove(); }
            catch (error) { showToast(`Undo failed: ${error.message}`, { type: 'error' }); }
        }, { once: true });
        toast.appendChild(action);
    }
    dom.toastRegion.appendChild(toast);
    timer = setTimeout(() => { toast.remove(); timer = null; }, 6000);
}

export const toast = Object.freeze({
    success: (message, options) => showToast(message, { ...options, type: 'success' }),
    error: (message, options) => showToast(message, { ...options, type: 'error' }),
    info: (message, options) => showToast(message, { ...options, type: 'info' }),
});
