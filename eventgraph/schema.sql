-- EventGraph ontology schema (idempotent)
create table if not exists onto_class (
  id   smallint primary key,
  name text unique not null
);
create table if not exists onto_relation_type (
  id          smallint primary key,
  name        text unique not null,
  src_class   text,          -- advisory only, not enforced
  dst_class   text,
  description text
);

create table if not exists run (
  id                text primary key,
  event_name        text not null,
  date_from         date not null,
  date_to           date not null,
  seed_persons      text[] not null default '{}',
  seed_orgs         text[] not null default '{}',
  seed_countries    text[] not null default '{}',
  expanded_keywords text[] not null default '{}',
  status            text not null default 'created',
  error             text,
  doc_selected      int not null default 0,
  doc_extracted     int not null default 0,
  doc_failed        int not null default 0,
  tokens_in         bigint not null default 0,
  tokens_out        bigint not null default 0,
  started_at        timestamptz not null default now(),
  finished_at       timestamptz
);
create table if not exists run_document (
  id           bigserial primary key,
  run_id       text not null references run(id),
  source_table text not null,
  source_idx   bigint not null,
  title        text not null,
  url          text not null default '',
  press        text not null default '',
  writedate    timestamp not null,
  score        double precision not null default 0,
  rank         int not null default 0,
  status       text not null default 'selected',
  unique (run_id, source_table, source_idx)
);
create table if not exists entity (
  id            bigserial primary key,
  run_id        text not null references run(id),
  class_id      smallint not null references onto_class(id),
  name_ko       text not null,
  norm_key      text not null,
  surface_forms text[] not null default '{}',
  doc_count     int not null default 0,
  degree        int not null default 0
);
create index if not exists entity_run_idx on entity(run_id);
create table if not exists affiliation (
  run_id     text not null references run(id),
  person_id  bigint not null references entity(id),
  target_id  bigint not null references entity(id),
  role       text not null default '',
  doc_count  int not null default 0,
  is_primary boolean not null default false
);
create table if not exists relation (
  id        bigserial primary key,
  run_id    text not null references run(id),
  type_id   smallint not null references onto_relation_type(id),
  src_id    bigint not null references entity(id),
  dst_id    bigint not null references entity(id),
  raw_label text not null default '',
  doc_count int not null default 0,
  date_min  date,
  date_max  date
);
create index if not exists relation_run_idx on relation(run_id);
create table if not exists event (
  id          bigserial primary key,
  run_id      text not null references run(id),
  date        date,
  summary_ko  text not null,
  location_id bigint references entity(id),
  doc_count   int not null default 0
);
create table if not exists event_participant (
  event_id  bigint not null references event(id),
  entity_id bigint not null references entity(id)
);
create table if not exists evidence (
  id              bigserial primary key,
  run_id          text not null references run(id),
  relation_id     bigint references relation(id),
  event_id        bigint references event(id),
  run_document_id bigint not null references run_document(id),
  sentence        text not null,
  evidence_date   date
);
create index if not exists evidence_rel_idx on evidence(relation_id);
create index if not exists evidence_evt_idx on evidence(event_id);

insert into onto_class(id, name) values
  (1,'PERSON'),(2,'ORG'),(3,'LOCATION'),(4,'COUNTRY'),(5,'EVENT')
on conflict do nothing;

insert into onto_relation_type(id, name, src_class, dst_class, description) values
  (1,'MEMBER_OF','PERSON','ORG|COUNTRY','소속'),
  (2,'LEADS','PERSON','ORG|COUNTRY','수장'),
  (3,'REPRESENTS','PERSON','ORG|COUNTRY','대표'),
  (4,'MEETS','PERSON','PERSON','회담·접촉'),
  (5,'CRITICIZES','PERSON','PERSON','비난·비판'),
  (6,'SUPPORTS','PERSON','PERSON','지지'),
  (7,'NEGOTIATES_WITH','PERSON','PERSON','협상'),
  (8,'ORDERS','PERSON','PERSON','지시·명령'),
  (9,'SUCCEEDS','PERSON','PERSON','후임'),
  (10,'ALLIES_WITH','ORG|COUNTRY','ORG|COUNTRY','협력·동맹'),
  (11,'SANCTIONS','ORG|COUNTRY','ORG|COUNTRY','제재'),
  (12,'ATTACKS','ORG|COUNTRY','ORG|COUNTRY','공격'),
  (13,'AGREES_WITH','ORG|COUNTRY','ORG|COUNTRY','합의'),
  (14,'PARTICIPATES','EVENT','*','참여'),
  (15,'OCCURS_AT','EVENT','LOCATION|COUNTRY','장소'),
  (16,'OTHER','*','*','스키마 밖 관계(raw_label 보존)'),
  (17,'WARNS','*','*','경고·위협'),
  (18,'REJECTS','*','*','반박·거부·항의'),
  (19,'PRESSURES','*','*','압박·요구·촉구'),
  (20,'MEDIATES','*','*','중재·주선'),
  (21,'REPORTS','*','*','보도·발표·주장')
on conflict do nothing;
