import os
import random
from pathlib import Path
from typing import Union
import duckdb
import folium
import googlemaps
import pandas as pd
import polars as pl
import streamlit as st
from PIL import Image
from dotenv import load_dotenv
from streamlit_folium import st_folium

load_dotenv('.env')

API_KEY = os.getenv('API_KEY')
DB = os.getenv("DB")

TOKYO_CENTRE = (35.6814834559876, 139.7677621491435)

CATEGORY = {
    'restaurant': 'cutlery',
    'sight-seeing': 'camera',
    'hotel': 'bed',
    'shopping': 'shopping-cart'
}

WEEK = {
    "Monday": "darkred",
    "Tuesday": "pink",
    "Wednesday": "beige",
    "Thursday": "darkblue",
    "Friday": "darkgreen",
    "Saturday": "orange",
    "Sunday": "lightgray"
}


def get_suggestions(
        gmaps: googlemaps.client.Client,
        keyword: str
) -> list[dict[str, Union[str, tuple[int, int]]]]:
    """
    :param gmaps: googlemaps.client.Client object that contains the method
    :param keyword: search key that is entered by the user, which is fed to the gmaps.places_autocomplete()
    :return:a list of dictionary that contains place id, coordinates, name of the place, and addreess
    """

    return [
        {
            "place_id": result['place_id'],
            "lat": gmaps.geocode(place_id=result['place_id'])[0]['geometry']['location']['lat'],
            "lon": gmaps.geocode(place_id=result['place_id'])[0]['geometry']['location']['lng'],
            "place_name": result['structured_formatting']['main_text'],
            "address": result['structured_formatting'].get('secondary_text', 'Address not found')
        }
        for result in gmaps.places_autocomplete(keyword)
    ]


def update_table(
        conn:duckdb.DuckDBPyConnection,
        df: pd.DataFrame
) -> None:
    """
    :param conn: duckdb connection
    :param df: pandas dataframe that is
    :return: update the table according to the changes made to the data editor
    """

    for k, v in st.session_state.places_displayed.items():
        if len(v) > 0:
            match k:
                case 'edited_rows':
                    row_no = list(v.keys())[0]
                    col_to_change = list(v[row_no].keys())[0]
                    df.loc[row_no, col_to_change] = v[row_no][col_to_change]
                case 'deleted_rows':
                    row_no = v[0]
                    df.drop(row_no, inplace=True)

    conn.execute("""
        drop table if exists t_places;
        create table t_places as select * from df;
    """)


def set_centre(
        conn: duckdb.DuckDBPyConnection,
        df: pd.DataFrame
) -> tuple[float, float]:
    coordinates = conn.sql("select lat, lon from df where is_active").fetchall()
    active_cnt = len(coordinates)

    match active_cnt:
        case 0:
            return TOKYO_CENTRE
        case 1:
            return coordinates[0]
        case _:
            return sum([x for x, y in coordinates]) / active_cnt, sum([y for x, y in coordinates]) / active_cnt


def add_marker(
    lat: float,
    lon: float,
    name: str,
    day: str,
    category: str
) -> folium.Marker:
    """
    :param lat: latitude, obtained from Google Maps api
    :param lon: longtitude, obtained from Google Maps api
    :param name: name of the place
    :param day: the day on which the place is scheduled to visit
    :param category: category of the place
    :return: a folium.Marker object that is used to rendered on the map
    """

    return folium.Marker(
        location=[lat, lon],
        tooltip=name,
        popup=name,
        icon=folium.Icon(
            icon=CATEGORY.get(category, 'cloud'),
            color=WEEK.get(day, 'black')
        )
    )


def get_nearest_station(
    lat_origin: float,
    lon_origin: float,
    gmaps: googlemaps.client.Client
) -> dict[str, str]:
    """
    :param lat_origin: latitude of the origin
    :param lon_origin: longtitude of the origin
    :param gmaps: googlemaps.client.Client object that contains the method
    :return: a dictionary that contains name of the nearest station, its address, walking distance and time
    """

    origin = (lat_origin, lon_origin)

    nearest_station = gmaps.places_nearby(
        location=origin,
        type="train_station",
        rank_by="distance"
    )['results'][0]

    distance = gmaps.distance_matrix(
        origins=origin,
        destinations=nearest_station['geometry']['location'],
        mode="walking"
    )

    return {
        "station": nearest_station['name'],
        "address": " ".join(distance['destination_addresses']),
        "walking distance": distance["rows"][0]['elements'][0]['distance']['text'],
        "walking time": distance["rows"][0]['elements'][0]['duration']['text']
    }


def make_schedule(
        conn: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        day: str,
        gmaps: googlemaps.client.Client
) -> None:
    """
    :param conn: duckdb connection
    :param df: pandas dataframe that contains the current data in the data_editor
    :param day: name of the day in a week (Monday, Tuesday, etc)
    :param gmaps: googlemaps.client.Client object that contains the method
    :return: nothing, simply renders the schedule based on which day of the week
    """
    pl_df = conn.execute("select * from df order by day, trip_order").pl()

    place_ids = pl_df.filter(
        pl.col("day") == day
    ).select(
        [
            "lat",
            "lon",
            "name",
            "place_id"
        ]
    ).to_dict(as_series=False)

    ids_count = len(place_ids)
    col_count = ids_count * 2

    match ids_count:
        case 0:
            st.write("No schedule yet")
        case _:
            for i, (name, id, lat, lon, col) in enumerate(
                    zip(
                        place_ids['name'],
                        place_ids['place_id'],
                        place_ids['lat'],
                        place_ids['lon'],
                        st.columns(col_count, gap="medium")
                    )
            ):
                nearest_station = get_nearest_station(lat_origin=lat, lon_origin=lon, gmaps=gmaps)
                des_path = Path().cwd().joinpath('pictures', f"{name}")
                des_path.mkdir(exist_ok=True, parents=True)
                pic_ref = random.choice(gmaps.place(id)['result']['photos'])['photo_reference']
                pic_file_name = f"{name}_img.jpg"
                with open(f"{des_path.as_posix()}/{pic_file_name}", "wb") as f:
                    for chunk in gmaps.places_photo(photo_reference=pic_ref, max_width=300, max_height=300):
                        if chunk:
                            f.write(chunk)

                pic = Path().cwd().joinpath('pictures', name, pic_file_name).as_posix()
                with col:
                    with Image.open(pic) as im:
                        im.resize((300, 300))
                        st.image(im, caption=name)
                        with st.expander("Nearest Station: "):
                            st.write(f"{nearest_station['station']}")
                            st.caption(f"Distance: {nearest_station['walking distance']}")
                            st.caption(f"Time: {nearest_station['walking time']}")


def main() -> None:
    # Connect to google maps api
    gmaps = googlemaps.Client(key=API_KEY)

    # Connect to database
    conn = duckdb.connect(DB)

    # Set up map
    m = folium.Map(
        location=TOKYO_CENTRE,
        zoom_start=13
    )

    fg = folium.FeatureGroup(name="Locations")

    # Set up page
    st.set_page_config(
        page_title="Tokyo Trip Planner",
        layout="wide"
    )

    st.header(":jp: Tokyo Trip Planner :jp:", divider="rainbow")

    # Part 1 -> Enter keyword, pick the exact location:
    with st.chat_message("human"):
        st.write("⬇️ Search places ⬇️")
        # keyword
        st.text_input(
            label="Enter keyword and find the exact location",
            value='',
            key="keyword",
            placeholder='E.g. Tokyo Tower'
        )
        # note: everytime a new keyword is entered, selected location will be refreshed

        # Options
        if st.session_state['keyword']:
            with st.expander("See results: "):
                st.selectbox(
                    label="",
                    label_visibility="collapsed",
                    index=None,
                    placeholder="Pick the right location",
                    options=(
                        (
                            s['place_name'],
                            s['address'],
                            s['place_id'],
                            s['lat'],
                            s['lon']
                        )
                        for s in get_suggestions(gmaps=gmaps, keyword=st.session_state['keyword'])
                    ),
                    key="selected_location",
                    on_change=lambda: conn.execute("insert into t_places(name, address, place_id, lat, lon) values (?, ?, ?, ?, ?)",
                                               st.session_state.selected_location
                                               ).commit() if st.session_state.selected_location else None
                )


    # Part 2 -> Table updated automatically as the exact location is selected:
    with st.chat_message("human"):
        st.write("⬇️ Edit or remove schedule ⬇️")
        st.data_editor(
            data=conn.sql("select * from t_places;").df(),
            num_rows="dynamic",
            key="places_displayed",
            hide_index=True,
            width=1200,
            on_change=update_table,
            args=[conn, conn.sql("select * from t_places;").df()],
            column_config={
                "name": st.column_config.TextColumn(
                    "name",
                    disabled=True
                ),
                "address": st.column_config.TextColumn(
                    "address",
                    disabled=True
                ),
                "day": st.column_config.SelectboxColumn(
                    "day",
                    help="Pick a day",
                    options=WEEK.keys(),
                    required=True,
                ),
                "category": st.column_config.SelectboxColumn(
                    "category",
                    help="What kind of place is this",
                    options=CATEGORY.keys(),
                    required=True
                ),
                "place_id": st.column_config.TextColumn(
                    "place_id",
                    disabled=True
                ),
                "lat": st.column_config.NumberColumn(
                    "lat",
                    disabled=True
                ),
                "lon": st.column_config.NumberColumn(
                    "lon",
                    disabled=True
                )
            }
        )

    # Part 3 -> Map rerendered as table updated:
    with st.chat_message("human"):
        st.write("⬇️ Map ⬇️")
        location_markers = [(tup[0], tup[1], tup[2], tup[3], tup[4])
                            for tup in conn.execute("""
                                select
                                    distinct name
                                    , lat
                                    , lon
                                    , day
                                    , category
                                from
                                    t_places
                                where
                                    lat is not null
                                    and lon is not null
                                    and is_active
                            """).fetchall()]

        for name, lat, lon, day, category in location_markers:
            fg.add_child(
                add_marker(
                    lat=lat,
                    lon=lon,
                    name=name,
                    category=category,
                    day=day
                )
            )

        st_folium(
            m,
            feature_group_to_add=fg,
            center=set_centre(conn=conn, df=conn.sql("select * from t_places").df()),
            width=1000
        )

    df = conn.sql("select * from t_places").df()

    # Part 4 -> Generate schedule:
    with st.chat_message("human"):
        st.write("⬇️ Schedule ⬇️")

        mon, tue, wed, thur, fri, sat, sun = st.tabs(WEEK.keys())

        with mon:
            make_schedule(
                conn=conn,
                day='Monday',
                df=df,
                gmaps=gmaps
            )
        with tue:
            make_schedule(
                conn=conn,
                day='Tuesday',
                df=df,
                gmaps=gmaps
            )
        with wed:
            make_schedule(
                conn=conn,
                day='Wednesday',
                df=df,
                gmaps=gmaps
            )
        with thur:
            make_schedule(
                conn=conn,
                day='Thursday',
                df=df,
                gmaps=gmaps
            )
        with fri:
            make_schedule(
                conn=conn,
                day='Friday',
                df=df,
                gmaps=gmaps
            )
        with sat:
            make_schedule(
                conn=conn,
                day='Saturday',
                df=df,
                gmaps=gmaps
            )
        with sun:
            make_schedule(
                conn=conn,
                day='Sunday',
                df=df,
                gmaps=gmaps
            )


if __name__ == "__main__":
    main()

