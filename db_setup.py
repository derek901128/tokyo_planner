import duckdb
def main():
    conn = duckdb.connect("tokyo.db")

    conn.execute("""
        drop table if exists t_places;
        
        create table t_places (
            place_id varchar(200) primary key unique default '',
            name varchar(200),
            address varchar(200) default '',
            lat DOUBLE default 35.6814834559876,
            lon DOUBLE default 139.7677621491435,
            is_active bool default false,
            day varchar(15) default '',
            trip_order float default 1,
            category varchar(20) default ''
        );
    """)

    conn.execute("""
        insert into t_places(
            name,
            category
        ) values(
            'Tokyo Center', 
            'sight-seeing'
        );
    """).commit()

if __name__ == "__main__":
    main()