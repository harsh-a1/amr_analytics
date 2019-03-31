
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
		deAgg.categorycomboid as de_aggccid,
		max(deAgg.name) as de_aggname
	from programstageinstance psi
	inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
	inner join dataelement de on tedv.dataelementid = de.dataelementid
	inner join dataelement deAgg on tedv.value = left(deAgg.code,3)	
	where de.code = 'organism' and tedv.value != '' and deAgg.code like '%_AW'
	group by psi.programstageinstanceid,de.dataelementid,
		de.code,psi.organisationunitid,tedv.value,deAgg.dataelementid,executiondate
)organism

inner join

(
	select * 
	from
		(
		select sample.programstageinstanceid,	
			sample.de_sampleid,
			sample_coid,
			antibiotics_results.de_antibioticsid,
			antibiotics_results.antibiotics_coid
			
		from
		(
			select psi.programstageinstanceid,
			psi.organisationunitid as sourceid,
			de.dataelementid as de_sampleid,
			tedv.value as samplevalue,
			deco.categoryoptionid as sample_coid,
			deco.uid as sample_couid,
			max(deco.name) as sample_coname
			from programstageinstance psi
			inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
			inner join dataelement de on tedv.dataelementid = de.dataelementid
			inner join dataelementcategoryoption deco on deco.code = tedv.value
			where de.uid = 'mp5MeJ2dFQz' and tedv.value != '' 
			group by psi.programstageinstanceid,de.dataelementid,de.code,
			psi.organisationunitid,tedv.value,deco.categoryoptionid
		)sample
		inner join 
		(
			select antibiotics.*,
				deco.categoryoptionid as antibiotics_coid,
				deco.uid as antibiotics_result_co_uid,
				deco.name as antibiotics_result_co_name	
			from(
				select psi.programstageinstanceid,
				psi.organisationunitid as sourceid,
				de.dataelementid as de_antibioticsid,
				de.code as de_antibioticscode,
				max(de.name) as antibioticsname,
				tedv.value,
				concat(de.code,'_',left(value,1)) as co_antibioticresult_code
				from programstageinstance psi
				inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
				inner join dataelement de on tedv.dataelementid = de.dataelementid
				inner join dataelementgroupmembers degm on degm.dataelementid = de.dataelementid 
				inner join dataelementgroup deg on degm.dataelementgroupid = deg.dataelementgroupid
				where deg.uid = 'UqaPXt3CGcz' and tedv.value != '' 
				group by psi.programstageinstanceid,de.dataelementid,de.code,
				psi.organisationunitid,tedv.value
			)antibiotics
			inner join dataelementcategoryoption deco on deco.code = co_antibioticresult_code
		)antibiotics_results
		on sample.programstageinstanceid = antibiotics_results.programstageinstanceid
		order by sample.programstageinstanceid 
	)antibiotic_sample
	inner join 
	(
	select coc_co.categoryoptioncomboid,array_agg(coc_co.categoryoptionid) as cocelems,cc.categorycomboid,max(cc.name) as ccname
	from categoryoptioncombos_categoryoptions coc_co
	inner join categorycombos_optioncombos cc_coc on cc_coc.categoryoptioncomboid = coc_co.categoryoptioncomboid
	inner join categorycombo cc on cc.categorycomboid = cc_coc.categorycomboid
	where cc_coc.categorycomboid != 102161
	group by coc_co.categoryoptioncomboid,cc.categorycomboid
	)cocs
	on antibiotic_sample.sample_coid = any(cocs.cocelems) and antibiotic_sample.antibiotics_coid = any(cocs.cocelems)
)antibiotic_sample
on organism.programstageinstanceid = antibiotic_sample.programstageinstanceid and organism.de_aggccid = antibiotic_sample.categorycomboid
left join period p on p.startdate = organism.startdate and p.enddate = organism.enddate
group by de_aggid,organism.sourceid,organism.startdate,organism.enddate,categoryoptioncomboid,p.periodid
order by deid