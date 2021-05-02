/**
 * from https://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
 * returns mortons number for provided tile
 */
function tileToZIndex(x,y) {
    let zIndex = 0;
    const bounds = (Math.max(x,y) >>> 0).toString(2).length;

    for (let i = 0; i < bounds; ++i)
        zIndex |= (x & 1 << i) << i | (y & 1 << i) << (i + 1);

    return zIndex >>> 0;
}

exports.tileToZIndex = tileToZIndex;