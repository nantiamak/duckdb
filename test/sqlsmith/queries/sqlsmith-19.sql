insert into main.orders values ( 27, 98, cast(null as VARCHAR), cast(null as DECIMAL), cast(null as DATE), case when (EXISTS ( select ref_0.r_regionkey as c0, ref_0.r_comment as c1, ref_0.r_regionkey as c2, ref_0.r_name as c3, ref_0.r_comment as c4, ref_0.r_comment as c5 from main.region as ref_0 where ref_0.r_name is NULL)) and (62 is not NULL) then cast(null as VARCHAR) else cast(null as VARCHAR) end , cast(null as VARCHAR), 97, cast(null as VARCHAR)) 
