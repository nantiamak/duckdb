insert into main.lineitem values ( 95, 82, 42, 100, 53, cast(null as DECIMAL), cast(null as DECIMAL), default, default, default, case when (((0) and ((EXISTS ( select ref_0.l_shipdate as c0, (select s_suppkey from main.supplier limit 1 offset 6) as c1, ref_0.l_quantity as c2, ref_0.l_suppkey as c3, ref_0.l_comment as c4 from main.lineitem as ref_0 where ref_0.l_comment is not NULL)) or (78 is not NULL))) or (26 is NULL)) or ((98 is not NULL) and (75 is not NULL)) then case when ((98 is NULL) or (62 is not NULL)) or (16 is not NULL) then cast(coalesce(cast(null as DATE), cast(null as DATE)) as DATE) else cast(coalesce(cast(null as DATE), cast(null as DATE)) as DATE) end else case when ((98 is NULL) or (62 is not NULL)) or (16 is not NULL) then cast(coalesce(cast(null as DATE), cast(null as DATE)) as DATE) else cast(coalesce(cast(null as DATE), cast(null as DATE)) as DATE) end end , case when 5 is not NULL then cast(null as DATE) else cast(null as DATE) end , cast(null as DATE), cast(null as VARCHAR), default, cast(nullif(cast(null as VARCHAR), cast(null as VARCHAR)) as VARCHAR)) 
