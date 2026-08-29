import { api } from '../api/client.js';
import { dom } from './dom.js';
import { store } from '../state/store.js';
import { clear, option } from '../utils/dom.js';
import { normalizeIds } from '../utils/data.js';
import { toast } from './toast.js';

let pendingIds = [];
let previousFocus = null;
let onSaved = async () => {};

function isOpen() { return dom.modal.backdrop?.classList.contains('is-open'); }
function focusables() { return [...dom.modal.backdrop.querySelectorAll('button:not([disabled]),select:not([disabled]),[tabindex]:not([tabindex="-1"])')]; }

function close() {
    pendingIds = [];
    dom.modal.backdrop.classList.remove('is-open');
    dom.modal.backdrop.setAttribute('aria-hidden', 'true');
    if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
    previousFocus = null;
}

async function renderExisting(lists) {
    clear(dom.modal.existing);
    if (!lists.length) { dom.modal.existing.textContent = '—'; return; }
    const fragment = document.createDocumentFragment();
    for (const list of lists) {
        const chip = document.createElement('span'); chip.className = 'modal-chip';
        const dot = document.createElement('span'); dot.className = 'modal-chip-dot'; dot.style.background = list.color || '#1d1d1f';
        const label = document.createElement('span'); label.textContent = list.name;
        const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'modal-chip-remove'; remove.textContent = '×';
        remove.dataset.action = 'remove-list'; remove.dataset.listId = String(list.id); remove.setAttribute('aria-label', `Remove ${list.name} from selected articles`);
        chip.append(dot, label, remove); fragment.appendChild(chip);
    }
    dom.modal.existing.appendChild(fragment);
}

export async function openListModal(ids) {
    const normalized = normalizeIds(Array.isArray(ids) ? ids : [ids]);
    if (!normalized.length) return;
    if (!isOpen() && document.activeElement instanceof HTMLElement) previousFocus = document.activeElement;
    pendingIds = normalized;
    clear(dom.modal.select); option(dom.modal.select, '', 'Choose list');
    let existing = [];
    try {
        const payload = await api.articleLists(normalized);
        existing = normalized.length === 1 ? (payload?.listsByArticleId?.[String(normalized[0])] || []) : (payload?.commonLists || []);
    } catch { existing = []; }
    const existingIds = new Set(existing.map(list => String(list.id)));
    for (const list of store.reference.lists) {
        const node = option(dom.modal.select, list.id, existingIds.has(String(list.id)) ? `${list.name} (already)` : list.name);
        node.disabled = existingIds.has(String(list.id));
    }
    await renderExisting(existing);
    dom.modal.backdrop.classList.add('is-open'); dom.modal.backdrop.setAttribute('aria-hidden', 'false'); dom.modal.select.focus({ preventScroll: true });
}

export function initModal(options = {}) {
    onSaved = options.onSaved || onSaved;
    dom.modal.backdrop.tabIndex = -1;
    dom.modal.close.addEventListener('click', close);
    dom.modal.cancel.addEventListener('click', close);
    dom.modal.backdrop.addEventListener('click', async event => {
        if (event.target === dom.modal.backdrop) { close(); return; }
        const remove = event.target.closest('[data-action="remove-list"]');
        if (!remove) return;
        remove.disabled = true;
        try { await api.removeFromList(remove.dataset.listId, pendingIds); await openListModal(pendingIds); }
        catch (error) { remove.disabled = false; toast.error(`Remove from list failed: ${error.message}`); }
    });
    dom.modal.backdrop.addEventListener('keydown', event => {
        if (event.key === 'Escape') { event.preventDefault(); close(); return; }
        if (event.key !== 'Tab') return;
        const nodes = focusables(); if (!nodes.length) return;
        const [first, last] = [nodes[0], nodes.at(-1)];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    dom.modal.confirm.addEventListener('click', async () => {
        const listId = dom.modal.select.value;
        if (!listId || !pendingIds.length) { toast.info('Please choose a list.'); return; }
        dom.modal.confirm.disabled = true;
        try { await api.addToList(listId, pendingIds); close(); await onSaved(); toast.success('Saved to list'); }
        catch (error) { toast.error(error.message); }
        finally { dom.modal.confirm.disabled = false; }
    });
}
