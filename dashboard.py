import pandas as pd
from dash import Dash, dcc, html, Input, Output
from sqlalchemy import create_engine
import folium
import plotly.graph_objects as go



def run_app(database):

    username = 'postgres'
    password = 'your_password'
    host = 'localhost'       
    port = '5432'

    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')


    flights_df = pd.read_sql("SELECT * FROM flights", engine)
    aircraft_df = pd.read_sql("SELECT * FROM aircraft", engine)
    airlines_df = pd.read_sql("SELECT * FROM airlines", engine)
    airports_df = pd.read_sql("SELECT * FROM airports", engine)


    merged_df = flights_df.merge(
        aircraft_df, on='aircraft_icao', how='left'
    ).merge(
        airports_df.add_prefix("dep_"), left_on='departure_icao', right_on='dep_icao', how='left'
    ).merge(
        airports_df.add_prefix("arr_"), left_on='arrival_icao', right_on='arr_icao', how='left'
    )


    model_options = merged_df['model_name'].dropna().unique()
    model_options.sort()


    app = Dash(__name__)




    @app.callback(
        Output('map-frame', 'srcDoc'),
        Input('model-selector', 'value')
    )
    def update_map_by_model(selected_model):
        filtered_df = merged_df
        if selected_model:
            filtered_df = merged_df[merged_df['model_name'] == selected_model]

        flight_map = folium.Map(location=[44.5, 35.0], zoom_start=5)

        for _, row in filtered_df.iterrows():
            dep_coords = (row['dep_latitude'], row['dep_longitude'])
            arr_coords = (row['arr_latitude'], row['arr_longitude'])

            if None not in dep_coords and None not in arr_coords:
                folium.PolyLine(
                    locations=[dep_coords, arr_coords],
                    color='blue',
                    weight=2,
                    opacity=0.7,
                    tooltip=f"Flight {row['flight_icao']}"
                ).add_to(flight_map)

                folium.CircleMarker(dep_coords, radius=3, color='green', fill=True).add_to(flight_map)
                folium.CircleMarker(arr_coords, radius=3, color='red', fill=True).add_to(flight_map)

        return flight_map.get_root().render()



    model_counts = aircraft_df['model_name'].value_counts().nlargest(10)
    model_fig = go.Figure(data=[
        go.Bar(x=model_counts.index, y=model_counts.values)
    ])
    model_fig.update_layout(
        title='Top 10 Aircrafts models',
        xaxis_title='Model',
        yaxis_title='Frequency',
        height=400, width=500
    )


    airline_counts = airlines_df['airline_name'].value_counts().nlargest(10)
    airline_fig = go.Figure(data=[
        go.Bar(y=airline_counts.index, x=airline_counts.values, orientation='h')
    ])
    airline_fig.update_layout(
        title='Top 10 Airlines',
        xaxis_title='Airline',
        yaxis_title='Frequency',
        height=400, width=500
    )



    app.layout = html.Div([
        html.H1("Aircraft routes over the Black Sea"),

        html.Label("Choose aircraft model"),
        dcc.Dropdown(
            id='model-selector',
            options=[{'label': model, 'value': model} for model in model_options],
            value=None,
            placeholder="Choose aircraft model"
        ),

        html.Iframe(id='map-frame', width='100%', height='600'),

        html.Div([
            html.Div([
                dcc.Graph(figure=model_fig)
            ], style={'display': 'inline-block', 'width': '49%'}),
        
            html.Div([
                dcc.Graph(figure=airline_fig)
            ], style={'display': 'inline-block', 'width': '49%'})
        ])
    ])

    return app
