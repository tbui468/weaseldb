create table planets (img bytea);
insert into planets (img) values ('\xffffff'), ('\x00ff00'), ('\xff');
select img from planets;
drop table planets;
