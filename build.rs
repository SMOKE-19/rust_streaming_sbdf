fn main() {
    let mut build = cc::Build::new();
    build
        .include("vendor/sbdf-c/include")
        .define("SBDF_STATIC", None)
        .file("vendor/sbdf-c/src/metadata.c")
        .file("vendor/sbdf-c/src/valuearray.c")
        .file("vendor/sbdf-c/src/internals.c")
        .file("vendor/sbdf-c/src/fileheader.c")
        .file("vendor/sbdf-c/src/columnmetadata.c")
        .file("vendor/sbdf-c/src/bswap.c")
        .file("vendor/sbdf-c/src/sbdfstring.c")
        .file("vendor/sbdf-c/src/errors.c")
        .file("vendor/sbdf-c/src/object.c")
        .file("vendor/sbdf-c/src/columnslice.c")
        .file("vendor/sbdf-c/src/bytearray.c")
        .file("vendor/sbdf-c/src/tablemetadata.c")
        .file("vendor/sbdf-c/src/tableslice.c")
        .file("vendor/sbdf-c/src/valuetype.c");
    build.compile("sbdf_c");
}
