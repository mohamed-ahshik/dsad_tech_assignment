-- Normalized property + transaction schema (no jsonb).

create table public.properties (
  id uuid primary key default gen_random_uuid(),
  project text not null,
  street text,
  market_segment text not null
    constraint properties_market_segment_check
      check (market_segment in ('CCR', 'RCR', 'OCR')),
  x numeric,
  y numeric,
  created_at timestamptz not null default now()
);

comment on table public.properties is 'Project/property record; SVY21 x/y are the property address, not the transacted unit.';
comment on column public.properties.project is 'Name of the project.';
comment on column public.properties.street is 'Street the project is on.';
comment on column public.properties.market_segment is 'CCR, RCR, or OCR.';
comment on column public.properties.x is 'SVY21 x of property address.';
comment on column public.properties.y is 'SVY21 y of property address.';

create table public.property_transactions (
  id uuid primary key default gen_random_uuid(),
  property_id uuid not null references public.properties (id) on delete cascade,
  property_type text not null,
  district text not null,
  tenure text not null,
  type_of_sale smallint not null
    constraint property_transactions_type_of_sale_check
      check (type_of_sale in (1, 2, 3)),
  no_of_units integer not null
    constraint property_transactions_no_of_units_check
      check (no_of_units >= 1),
  price numeric not null,
  nett_price numeric,
  area numeric not null,
  type_of_area text not null
    constraint property_transactions_type_of_area_check
      check (type_of_area in ('Strata', 'Land', 'Unknown')),
  floor_range text not null,
  contract_date text not null,
  created_at timestamptz not null default now()
);

comment on table public.property_transactions is 'One row per transaction from the API transaction array.';
comment on column public.property_transactions.type_of_sale is '1 New Sale, 2 Sub Sale, 3 Resale.';
comment on column public.property_transactions.contract_date is 'Sale/option date as mmyy text, e.g. 0715.';

create index property_transactions_property_id_idx
  on public.property_transactions (property_id);

create index property_transactions_contract_date_idx
  on public.property_transactions (contract_date);

alter table public.properties enable row level security;
alter table public.property_transactions enable row level security;

create policy "properties_select_all"
  on public.properties
  for select
  to anon, authenticated
  using (true);

create policy "property_transactions_select_all"
  on public.property_transactions
  for select
  to anon, authenticated
  using (true);
