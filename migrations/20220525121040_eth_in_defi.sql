CREATE TABLE public.eth_in_defi (
	"timestamp" timestamptz NOT NULL,
	eth float8 NOT NULL,
	CONSTRAINT eth_in_defi_pk PRIMARY KEY ("timestamp")
);
