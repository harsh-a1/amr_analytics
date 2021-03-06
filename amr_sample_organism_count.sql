﻿
select de_aggid as deid,
	organism.sourceid,
	organism.startdate,
	organism.enddate,
	p.periodid,
	categoryoptioncomboid,
	count(organism.value) as value
from (
	select psi.programstageinstanceid,
		to_char(executiondate, 'yyyy-mm-01')::date as startdate,
		(date_trunc('month',executiondate)+interval '1 month' - interval '1 day')::date as enddate,
		psi.organisationunitid as sourceid,
		de.dataelementid as de_organismid,
		tedv.value,
		deAgg.dataelementid as de_aggid,
		max(deAgg.name) as de_aggname
	from programstageinstance psi
	inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
	inner join dataelement de on tedv.dataelementid = de.dataelementid
	inner join dataelement deAgg on tedv.value = deAgg.code	
	where de.code = 'organism' and tedv.value != ''
	group by psi.programstageinstanceid,de.dataelementid,
		de.code,psi.organisationunitid,tedv.value,deAgg.dataelementid,executiondate
)organism
inner join
(
	select psi.programstageinstanceid,
		psi.organisationunitid as sourceid,
		de.dataelementid as de_organismid,
		tedv.value,
		deco.categoryoptionid as decoid,
		deco.uid as decouid,

		max(deco.name) as deconame,
		coc.categoryoptioncomboid
	from programstageinstance psi
	inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
	inner join dataelement de on tedv.dataelementid = de.dataelementid
	inner join dataelementcategoryoption deco on deco.code = tedv.value
	inner join categoryoptioncombos_categoryoptions coc_co on coc_co.categoryoptionid = deco.categoryoptionid
	inner join categorycombos_optioncombos cc_coc on cc_coc.categoryoptioncomboid = coc_co.categoryoptioncomboid
	inner join categoryoptioncombo coc on coc.categoryoptioncomboid = cc_coc.categoryoptioncomboid
	where de.uid = 'mp5MeJ2dFQz' and tedv.value != '' and cc_coc.categorycomboid = 102161
	group by psi.programstageinstanceid,de.dataelementid,de.code,
		psi.organisationunitid,tedv.value,deco.categoryoptionid,
		coc.categoryoptioncomboid,coc.uid,deco.uid
)sample
on organism.programstageinstanceid = sample.programstageinstanceid
left join period p on p.startdate = organism.startdate and p.enddate = organism.enddate
group by de_aggid,organism.sourceid,organism.startdate,organism.enddate,categoryoptioncomboid,p.periodid
order by deid




