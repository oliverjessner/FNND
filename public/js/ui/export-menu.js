export function bindExportMenu({ root, trigger, popover, onSelect }) {
    let loading = false;
    const label = trigger?.querySelector('[data-export-label]');
    const formatButtons = [...(popover?.querySelectorAll('[data-export-format]') || [])];

    function setOpen(open) {
        if (!root || !trigger || !popover || loading) return;
        const next = Boolean(open);
        popover.hidden = !next;
        trigger.setAttribute('aria-expanded', String(next));
    }

    function setLoading(next) {
        loading = Boolean(next);
        trigger.disabled = loading;
        formatButtons.forEach(button => { button.disabled = loading; });
        if (label) label.textContent = loading ? 'Exporting…' : 'Export';
        if (loading) {
            popover.hidden = true;
            trigger.setAttribute('aria-expanded', 'false');
        }
    }

    trigger?.addEventListener('click', () => setOpen(popover.hidden));
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
        if (!popover?.hidden && !root?.contains(event.target)) setOpen(false);
    });
    window.addEventListener('keydown', event => {
        if (event.key !== 'Escape' || popover?.hidden) return;
        event.preventDefault();
        event.stopImmediatePropagation();
        setOpen(false);
        trigger?.focus();
    });

    return Object.freeze({ close: () => setOpen(false), setLoading });
}
