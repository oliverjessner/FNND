export function show(element) { element?.classList.remove('hide'); }
export function hide(element) { element?.classList.add('hide'); }
export function clear(element) { element?.replaceChildren(); }
export function text(element, value = '') { if (element) element.textContent = value == null ? '' : String(value); }

export function option(select, value, label) {
    const node = document.createElement('option');
    node.value = value;
    node.textContent = label;
    select.appendChild(node);
    return node;
}

export function setPressed(elements, predicate) {
    elements.forEach(element => {
        const active = Boolean(predicate(element));
        element.classList.toggle('is-active', active);
        element.setAttribute('aria-pressed', String(active));
    });
}
