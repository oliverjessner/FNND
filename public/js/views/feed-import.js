import { api } from '../api/client.js';
import { dom } from '../ui/dom.js';
import { toast } from '../ui/toast.js';
import { parseArticleImportText } from '../utils/import.js';

const MAX_IMPORT_FILE_BYTES = 512 * 1024;
let initialized = false;
let previousFocus = null;
let onImported = async () => {};

function focusables() {
    return [...dom.feedImport.backdrop.querySelectorAll('button:not([disabled]),textarea:not([disabled])')];
}

function setStatus(message) {
    dom.feedImport.status.textContent = message;
}

function close({ reset = true } = {}) {
    dom.feedImport.backdrop.classList.remove('is-open');
    dom.feedImport.backdrop.setAttribute('aria-hidden', 'true');
    if (reset) {
        dom.feedImport.urls.value = '';
        dom.feedImport.file.value = '';
        dom.feedImport.fileName.textContent = 'No file selected';
        setStatus('');
    }
    if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
    previousFocus = null;
}

function open() {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    dom.feedImport.backdrop.classList.add('is-open');
    dom.feedImport.backdrop.setAttribute('aria-hidden', 'false');
    dom.feedImport.urls.focus({ preventScroll: true });
}

function summary(result, parsed) {
    const parts = [`${Number(result.imported || 0).toLocaleString('en-US')} imported`];
    const duplicates = Number(result.duplicates || 0) + parsed.duplicates;
    const invalid = Number(result.invalid || 0) + parsed.invalid;
    if (duplicates) parts.push(`${duplicates.toLocaleString('en-US')} already existed or duplicated`);
    if (invalid) parts.push(`${invalid.toLocaleString('en-US')} invalid`);
    if (result.failed) parts.push(`${Number(result.failed).toLocaleString('en-US')} failed`);
    return parts.join(' · ');
}

async function importUrls() {
    const parsed = parseArticleImportText(dom.feedImport.urls.value);
    if (parsed.overflow) { setStatus('Maximum 500 URLs per import. Split the list into smaller files.'); return; }
    if (!parsed.urls.length) { setStatus(parsed.invalid ? 'No valid HTTP(S) URLs found.' : 'Paste at least one article URL.'); return; }

    dom.feedImport.confirm.disabled = true;
    setStatus(`Importing ${parsed.urls.length.toLocaleString('en-US')} ${parsed.urls.length === 1 ? 'article' : 'articles'}…`);
    try {
        const result = await api.importArticles(parsed.urls);
        const message = summary(result, parsed);
        close();
        await onImported(result);
        toast.success(message);
    } catch (error) {
        setStatus(`Import failed: ${error.message}`);
    } finally {
        dom.feedImport.confirm.disabled = false;
    }
}

async function loadTextFile(file) {
    if (!file) return;
    if (file.size > MAX_IMPORT_FILE_BYTES) { setStatus('TXT file is too large. Maximum size: 512 KB.'); return; }
    if (!file.name.toLowerCase().endsWith('.txt') && file.type !== 'text/plain') { setStatus('Choose a plain .txt file.'); return; }
    try {
        const content = await file.text();
        dom.feedImport.urls.value = content;
        dom.feedImport.fileName.textContent = file.name;
        const parsed = parseArticleImportText(content);
        setStatus(`${parsed.urls.length.toLocaleString('en-US')} valid ${parsed.urls.length === 1 ? 'URL' : 'URLs'} loaded${parsed.invalid ? ` · ${parsed.invalid} invalid` : ''}.`);
    } catch (error) {
        setStatus(`Could not read file: ${error.message}`);
    }
}

export function initFeedImport(options = {}) {
    if (initialized) return;
    initialized = true;
    onImported = options.onImported || onImported;
    dom.feedImport.trigger.addEventListener('click', open);
    dom.feedImport.close.addEventListener('click', () => close());
    dom.feedImport.cancel.addEventListener('click', () => close());
    dom.feedImport.chooseFile.addEventListener('click', () => dom.feedImport.file.click());
    dom.feedImport.file.addEventListener('change', () => void loadTextFile(dom.feedImport.file.files?.[0]));
    dom.feedImport.confirm.addEventListener('click', () => void importUrls());
    dom.feedImport.backdrop.addEventListener('click', event => { if (event.target === dom.feedImport.backdrop) close(); });
    dom.feedImport.backdrop.addEventListener('keydown', event => {
        if (event.key === 'Escape') { event.preventDefault(); close(); return; }
        if (event.key !== 'Tab') return;
        const nodes = focusables(); if (!nodes.length) return;
        const [first, last] = [nodes[0], nodes.at(-1)];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
}
