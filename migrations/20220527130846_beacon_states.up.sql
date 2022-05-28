CREATE TABLE beacon_states (
	slot integer NOT NULL,
	state_root text NOT NULL,
	CONSTRAINT beacon_states_pk PRIMARY KEY (state_root)
);

CREATE UNIQUE INDEX beacon_states_slot_idx ON public.beacon_states (slot);

CREATE TABLE beacon_blocks (
	block_root text NOT NULL,
	state_root text NOT NULL,
	parent_root text NULL,
	deposit_sum int8 NOT NULL,
	deposit_sum_aggregated int8 NOT NULL,
	CONSTRAINT beacon_blocks_pk PRIMARY KEY (block_root),
	CONSTRAINT beacon_blocks_parent_root_fk FOREIGN KEY (parent_root) REFERENCES public.beacon_blocks(block_root),
	CONSTRAINT beacon_blocks_state_root_un UNIQUE (state_root),
	CONSTRAINT beacon_blocks_state_root_fk FOREIGN KEY (state_root) REFERENCES public.beacon_states(state_root)
);

CREATE TABLE beacon_validators_balance (
	"timestamp" timestamptz NOT NULL,
	state_root text NOT NULL,
	gwei int8 NOT NULL,
	CONSTRAINT beacon_validators_balance_pk PRIMARY KEY ("timestamp"),
	CONSTRAINT beacon_validators_balance_state_root_un UNIQUE (state_root),
	CONSTRAINT beacon_validators_balance_state_root_fk FOREIGN KEY (state_root) REFERENCES public.beacon_states(state_root)
);

CREATE TABLE beacon_issuance (
	"timestamp" timestamptz NOT NULL,
	state_root text NOT NULL,
	gwei int8 NOT NULL,
	CONSTRAINT beacon_issuance_pk PRIMARY KEY ("timestamp"),
	CONSTRAINT beacon_issuance_state_root_un UNIQUE (state_root),
	CONSTRAINT beacon_issuance_state_root_fk FOREIGN KEY (state_root) REFERENCES public.beacon_states(state_root)
);
