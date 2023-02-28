from pg_data_etl import Database

db = Database.from_config("lts", "localhost")


def generate_topology(network_table:str, where_statement:str, unique_id_field:str, target_table:str, srid:int=26918, hour_distance:float=16000, topology_threshold:float=0.0005, ):

    db.execute(
            
        f"""drop table if exists {target_table} CASCADE;
            create table {target_table} as select * from {network_table} a where {where_statement};
            alter table {target_table} add column source integer;
            alter table {target_table} add column target integer;
            select pgr_createTopology('{target_table}', 0.0005, 'geom', '{unique_id_field}');
            create or replace view {target_table}nodes as 
                select id, st_centroid(st_collect(pt)) as geom
                from (
                    (select source as id, st_startpoint(geom) as pt
                    from {target_table}
                    ) 
                union
                (select target as id, st_endpoint(geom) as pt
                from {target_table}
                ) 
                ) as foo
                group by id;
            alter table {target_table} add column length_m integer;
            update {target_table} set length_m = st_length(st_transform(geom,26918));
            alter table {target_table} add column traveltime_min double precision;
            update {target_table} set traveltime_min = length_m  / 16000.0 * 60; -- 16 kms per hr, about 10 mph. low range of beginner cyclist speeds
"""
 
    def __create_isochrone(self, travel_time:int=15):
        """
        Creates isochrone based on study_segment
        """
        db.execute(f"""
        drop materialized view if exists isochrone;
        create materialized view isochrone as 
            with nodes as (
             SELECT *
              FROM pgr_drivingDistance(
                'SELECT dvrpc_id as id, source, target, traveltime_min as cost FROM lts2gaps',
                array((select "source" from lts{self.highest_comfort_level}nodes a
                     inner join lts{self.highest_comfort_level}gaps b
                     on a.id = b."source" 
                     where b.dvrpc_id in {self.segment_ids})), 
                 {travel_time}, false) as di
             JOIN lts2nodes pt
             ON di.node = pt.id)
             select 1 as uid, st_concavehull(st_union(b.geom), .8) as geom from nodes a
             inner join lts2gaps b 
             on a.id = b."source" 
                   """)


