-- 1. launch Cassandra container: `docker run --name my-cassandra cassandra`
-- 2. get the container ID from `docker ps` (something that looks like fc2a2f580e81)
-- 3. on another term, launch `docker exec -it fc2a2f580e81 cqlsh`
-- 4. use the commands below...

CREATE KEYSPACE esipe WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE student (
    id varchar,
    name varchar,
    age int,
    primary key (id))

insert into student (id, name, age) values ('190AC', 'John Doe', 22)
