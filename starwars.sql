
--drop database if exists starwars;

-- pinver is already a superuser here on the mac
--create database starwars owner pinver;
\c starwars

-- let's create a role
drop role if exists jedi;
create role jedi with password 'force' login;
create role padawan with password 'force' login;

drop schema if exists public cascade;
create schema public;


drop table if exists people;
create table people (
    name text not null primary key,
    gender text not null,
    photo bytea not null,
    height real not null
);

drop table if exists species;
create table species (
    name text not null primary key,
    average_lifespan integer not null
);

drop table if exists planets;
create table planets (
    name text not null primary key,
    population bigint not null
);

drop table if exists landings;
create table landings (
    person_name text not null,
    planet_name text not null,
    landings integer not null,
    primary key(person_name, planet_name)
);

drop table if exists weapons;
create table weapons (
    weapon_id serial not null primary key,
    weapon text not null unique
);

drop table if exists starships;
create table starships (
    name text not null primary key,
    hyperdrive_rating real not null,
    weapon_id integer not null references weapons
);

create table council (
    name text not null references people(name)
);


grant all privileges on schema public TO jedi, padawan;
grant usage, select on all sequences in schema public to jedi, padawan;
grant all privileges on table people, species, planets, landings, weapons, starships, council to jedi;
grant all privileges on table people, species, planets, landings, weapons, starships to padawan;

insert into people values 
    ('Luke', 'male', E'\\xDEADBEEF', 1.72);

insert into species values
    ('Wookiee', 400);

insert into planets values
    ('Tatooine', 120000);

insert into landings values
    ('Luke', 'Tatooine', 1);

insert into weapons values
    (default, 'no weapon'), (default, 'sonic missiles');

insert into starships values
    ('NanShip', 'NaN', 1);
