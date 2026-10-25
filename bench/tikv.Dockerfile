FROM pingcap/tikv:v8.5.4 AS upstream
FROM rust:1.85.1-bookworm AS symbols
COPY --from=upstream /tikv-server /tikv-server
# The ARM upstream executable contains compressed DWARF. Its startup backtrace
# cache decompresses about 1.8 GiB of it. Keep executable code byte-identical.
RUN objcopy --only-section=.text -O binary /tikv-server /tmp/text-before \
    && sha256sum /tikv-server /tmp/text-before > /build-evidence.txt \
    && strip --strip-debug /tikv-server \
    && objcopy --only-section=.text -O binary /tikv-server /tmp/text-after \
    && cmp /tmp/text-before /tmp/text-after \
    && sha256sum /tikv-server /tmp/text-after >> /build-evidence.txt
FROM upstream
COPY --from=symbols /tikv-server /tikv-server
COPY --from=symbols /build-evidence.txt /cairn-build-evidence.txt
