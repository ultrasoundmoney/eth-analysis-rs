# This file is used to build the docker image for the server.
# It makes sure to split the build into two stages:
# 1. Build dependencies and cache them.
# 2. Build the executable and copy it into a minimal runtime image.
# For context on the approach see:
# https://stackoverflow.com/questions/58473606/cache-rust-dependencies-with-docker-build

FROM rust as builder
WORKDIR /app

# Build deps. We replace our main.rs with a dummy.rs to avoid rebuilding the
# main executable, creating a cached layer for the dependencies.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src/
RUN echo "fn main() {}" > dummy.rs
RUN sed -i 's#src/bin/serve.rs#dummy.rs#' Cargo.toml
RUN cargo build --release --bin serve

# Build executables.
RUN sed -i 's#dummy.rs#src/bin/serve.rs#' Cargo.toml
COPY src ./src
COPY .sqlx ./.sqlx
COPY migrations ./migrations
RUN cargo build --release --bin phoenix-service
RUN cargo build --release --bin record-eth-price
RUN cargo build --release --bin sync-beacon-states
RUN cargo build --release --bin sync-execution-blocks
RUN cargo build --release --bin sync-execution-supply-deltas
RUN cargo build --release --bin update-effective-balance-sum
RUN cargo build --release --bin update-issuance-breakdown
RUN cargo build --release --bin update-issuance-estimate
RUN cargo build --release --bin update-supply-projection-inputs
RUN cargo build --release --bin update-validator-rewards
# serve changes the most, put it last.
RUN cargo build --release --bin serve

# Build runtime image.
FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app

COPY --from=builder /app/target/release/phoenix-service /usr/local/bin
COPY --from=builder /app/target/release/record-eth-price /usr/local/bin
COPY --from=builder /app/target/release/serve /usr/local/bin
COPY --from=builder /app/target/release/sync-beacon-states /usr/local/bin
COPY --from=builder /app/target/release/sync-execution-blocks /usr/local/bin
COPY --from=builder /app/target/release/sync-execution-supply-deltas /usr/local/bin
COPY --from=builder /app/target/release/update-effective-balance-sum /usr/local/bin
COPY --from=builder /app/target/release/update-issuance-breakdown /usr/local/bin
COPY --from=builder /app/target/release/update-issuance-estimate /usr/local/bin
COPY --from=builder /app/src/bin/update-supply-projection-inputs/in_contracts_by_day.json /app/src/bin/update-supply-projection-inputs/in_contracts_by_day.json
COPY --from=builder /app/target/release/update-supply-projection-inputs /usr/local/bin
COPY --from=builder /app/target/release/update-validator-rewards /usr/local/bin

EXPOSE 3002
ENTRYPOINT ["/usr/local/bin/serve"]
