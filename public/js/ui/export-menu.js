export function bindExportMenu({ root, trigger, popover, onSelect }) {
    let loading = false;
    const usesDetails = root?.tagName === 'DETAILS';
    const label = trigger?.querySelector('[data-export-label]');
    const formatButtons = [...(popover?.querySelectorAll('[data-export-format]') || [])];

    function isOpen() {
        return usesDetails ? root.open : !popover.hidden;
    }

    function setOpen(open, { force = false } = {}) {
        if (!root || !trigger || !popover || (loading && !force)) return;
        const next = Boolean(open);
        if (usesDetails) root.open = next;
        else popover.hidden = !next;
        trigger.setAttribute('aria-expanded', String(next));
    }

    function setLoading(next) {
        loading = Boolean(next);
        trigger.disabled = loading;
        trigger.setAttribute('aria-disabled', String(loading));
        formatButtons.forEach(button => { button.disabled = loading; });
        if (label) label.textContent = loading ? 'Exporting…' : 'Export';
        if (loading) setOpen(false, { force: true });
    }

    if (usesDetails) root.addEventListener('toggle', () => trigger.setAttribute('aria-expanded', String(root.open)));
    else trigger?.addEventListener('click', () => setOpen(popover.hidden));
    popover?.addEventListener('click', async event => {
        const option = event.target.closest('[data-export-format]');
        if (!option || loading) return;
        const format = option.dataset.exportFormat;
        setOpen(false);
        setLoading(true);
        try { await onSelect(format); }
        finally { setLoading(false); }
    });
    document.addEventListener('pointerdown', event => {
        if (isOpen() && !root?.contains(event.target)) setOpen(false);
    });
    window.addEventListener('keydown', event => {
        if (event.key !== 'Escape' || !isOpen()) return;
        event.preventDefault();
        event.stopImmediatePropagation();
        setOpen(false);
        trigger?.focus();
    });

    return Object.freeze({ close: () => setOpen(false), setLoading });
}
