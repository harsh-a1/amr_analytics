create or replace function sampleWiseOrganism() returns setof record as '

select de_aggid as deid,
	organism.sourceid,
	organism.startdate,
	organism.enddate,
	p.periodid,
	categoryoptioncomboid,
	count(organism.value) as value
	from (
	select psi.programstageinstanceid,
		to_char(executiondate, $$yyyy-mm-01$$)::date as startdate,
		(date_trunc($$month$$,executiondate)+interval $$1 month$$ - interval $$1 day$$)::date as enddate,
		psi.organisationunitid as sourceid,
		de.dataelementid as de_organismid,
		tedv.value,
		deAgg.dataelementid as de_aggid,
		deAgg.categorycomboid as de_aggccid,
		max(deAgg.name) as de_aggname
	from programstageinstance psi
	inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
	inner join dataelement de on tedv.dataelementid = de.dataelementid
	inner join attributevalue av on tedv.value = av.value
	inner join dataelementattributevalues deav on deav.attributevalueid= av.attributevalueid
	inner join dataelement deAgg on deAgg.dataelementid = deav.dataelementid	
	where de.code = $$organism$$ and tedv.value != $$$$ and right(deAgg.code,3) !=$$_AW$$
	group by psi.programstageinstanceid,de.dataelementid,
		de.code,psi.organisationunitid,tedv.value,deAgg.dataelementid,executiondate
)organism
inner join
(
	select sample.programstageinstanceid,		
	array[sample_coid,loc_coid] as sample_location_coids,
	sample_coid,
	loc_coid
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
	where de.uid = $$mp5MeJ2dFQz$$ and tedv.value != $$$$
	group by psi.programstageinstanceid,de.dataelementid,de.code,psi.organisationunitid,tedv.value,deco.categoryoptionid
	)sample
	inner join
	(
	select psi.programstageinstanceid,
		psi.organisationunitid as sourceid,
		de.dataelementid as de_locid,
		tedv.value as locvalue,
		deco.categoryoptionid as loc_coid,
		deco.uid as loc_couid,
		max(deco.name) as sample_coname
		from programstageinstance psi
	inner join trackedentitydatavalue tedv on tedv.programstageinstanceid = psi.programstageinstanceid
	inner join dataelement de on tedv.dataelementid = de.dataelementid
	inner join dataelementcategoryoption deco on deco.code = tedv.value
	where de.code = $$WARD_TYPE$$ and tedv.value != $$$$
	group by psi.programstageinstanceid,de.dataelementid,de.code,psi.organisationunitid,tedv.value,deco.categoryoptionid
	)loc
	on loc.programstageinstanceid = sample.programstageinstanceid
)sample_location
on organism.programstageinstanceid = sample_location.programstageinstanceid
inner join
(
select coc_co.categoryoptioncomboid,array_agg(coc_co.categoryoptionid) as cocelems,cc.categorycomboid,max(cc.name) as ccname
from categoryoptioncombos_categoryoptions coc_co
inner join categorycombos_optioncombos cc_coc on cc_coc.categoryoptioncomboid = coc_co.categoryoptioncomboid
inner join categorycombo cc on cc.categorycomboid = cc_coc.categorycomboid
group by coc_co.categoryoptioncomboid,cc.categorycomboid
)cocs
on 	sample_location.sample_coid = any (cocs.cocelems) and 
	sample_location.loc_coid = any (cocs.cocelems) and 
	organism.de_aggccid = cocs.categorycomboid
left join period p on p.startdate = organism.startdate and p.enddate = organism.enddate
group by de_aggid,organism.sourceid,organism.startdate,organism.enddate,categoryoptioncomboid,p.periodid
order by deid


' Language sql;
CREATE or replace FUNCTION cs() RETURNS void AS $$
declare
dv record;
monthlyperiodtypeid integer;

BEGIN
    RAISE NOTICE 'Starting analytics generation...';
	DROP TABLE IF EXISTS inshallah;

	execute format('create temporary table inshallah as (select deid dataelementid,periodid,sourceid,
			coc categoryoptioncomboid,20 attributeoptioncomboid,value,''amr_analytics''::character varying storedby,now() created,now() lastupdated,
			NULL::character varying as comment,false followup,false deleted,startdate,enddate
			from sampleWiseOrganism()
			as so(deid integer,sourceid integer,startdate date,enddate date,periodid integer,coc integer,value bigint))');

	
	for dv in select distinct startdate,enddate from inshallah where periodid is NULL order by startdate
	loop		
		raise notice '%s',dv;
		INSERT INTO period(
		periodid, periodtypeid, startdate, enddate)
		VALUES (nextval('hibernate_sequence'), (select periodtypeid from periodtype where name='Monthly'), dv.startdate, dv.enddate);
	end loop;

	insert into datavalue select dataelementid,p.periodid,sourceid,
			categoryoptioncomboid,attributeoptioncomboid,value,storedby,created,lastupdated,
			comment,followup,deleted 
			from inshallah ins
			inner join period p on p.startdate=ins.startdate and p.enddate=ins.enddate
			where p.periodtypeid=(select periodtypeid from periodtype where name='Monthly')
	ON CONFLICT ON CONSTRAINT datavalue_pkey
	DO update set value=excluded.value,
			lastupdated=now(),
			storedby='amr_analytics';
	  
END;
$$ LANGUAGE plpgsql;

select cs();

