from flask import Response
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
from flask_caching import Cache
import orjson

# Load your data
new_data = pd.read_parquet('new_data.parquet', engine='pyarrow')

# Convert appropriate columns to categorical
categorical_cols = ['GLabel', 'Label', 'SubCatLbl']
new_data[categorical_cols] = new_data[categorical_cols].astype('category')

# Define the desired order
desired_order = [
    'Doctoral',
    "Master's",
    "Bachelor's",
    'Associates',
    'SF: 2Yr',
    'SF: 4Yr',
    'Tribal/Oth',
    'Not in'
]

def get_unique_labels_for_year_glabel(year, glabel, data_frame):
    filtered_df = data_frame[(data_frame['YEAR'] == year) & (data_frame['GLabel'] == glabel)]
    unique_labels = filtered_df.sort_values(by='NewLabel')['Label'].unique().tolist()
    return unique_labels

# Initialize Dash app and expose server
app = dash.Dash(__name__)
server = app.server

# Configure server-side caching
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory',
    'CACHE_THRESHOLD': 100
})

# Cache the preprocessing step and assign globally
@cache.memoize(timeout=3600)
def prepare_data():
    new_df_with_unique_labels = new_data.drop_duplicates(subset=['YEAR', 'GLabel', 'Label'])
    sorted_df = new_df_with_unique_labels.sort_values(by='SubCatLbl')
    sorted_df['GLabel'] = pd.Categorical(sorted_df['GLabel'], categories=desired_order, ordered=True)
    sorted_df = sorted_df.sort_values('GLabel')
    sorted_df.to_parquet('preprocessed_data.parquet', engine='pyarrow')
    all_glabels_sorted = sorted_df['GLabel'].unique()
    year_glabel_mapping = new_df_with_unique_labels.groupby('YEAR')['GLabel'].unique().to_dict()
    return new_df_with_unique_labels, all_glabels_sorted, year_glabel_mapping

@cache.memoize(timeout=3600)
def load_data():
    df = pd.read_parquet('preprocessed_data.parquet', engine='pyarrow')
    df['GLabel'] = pd.Categorical(df['GLabel'], categories=desired_order, ordered=True)
    year_glabel_mapping = df.groupby('YEAR')['GLabel'].unique().to_dict()
    all_glabels = df['GLabel'].unique()
    return df, all_glabels, year_glabel_mapping

# Assign the cached (or computed) values to global variables
new_df_with_unique_labels, all_glabels_sorted, year_glabel_mapping = prepare_data()

app.layout = html.Div([
    dcc.Dropdown(
        id='most-recent-name-dropdown',
        options=[{'label': name, 'value': name} for name in new_data['MostRecentName'].unique()],
        placeholder="Select a MostRecentName"
    ),
    html.Div(id='link-unit-display'),
    html.Table(id='year-glabel-table')
])

@app.server.route('/get_data')
def get_data():
    return Response(
        orjson.dumps(new_data.to_dict(orient="records")),
        mimetype='application/json'
    )

app.config.suppress_callback_exceptions = True

@cache.memoize(timeout=300)
def get_filtered_data(selected_most_recent_name):
    return new_data[new_data['MostRecentName'] == selected_most_recent_name]

@app.callback(
    [Output('year-glabel-table', 'children'),
     Output('link-unit-display', 'children')],
    [Input('most-recent-name-dropdown', 'value')],
    prevent_initial_call=True
)
def update_table(selected_most_recent_name):
    years = sorted(new_data['YEAR'].unique())
    instnm_by_year = get_filtered_data(selected_most_recent_name).groupby('YEAR')['Instnm'].first().to_dict()

    filtered_data = get_filtered_data(selected_most_recent_name)
    link_unit_exists = 'LinkUnit' in filtered_data.columns and not filtered_data['LinkUnit'].isnull().all()

    table_header_cells = [html.Th("GLabel")] + [html.Th(year) for year in years]
    if link_unit_exists:
        table_header_cells.append(html.Th("LinkUnit"))
    table_header = html.Tr(table_header_cells)

    instnm_row_cells = [html.Th("Instnm")] + [html.Th(instnm_by_year.get(year, 'N/A')) for year in years]
    if link_unit_exists:
        link_unit_value = filtered_data['LinkUnit'].iloc[0]
        instnm_row_cells.append(html.Th(link_unit_value))
    instnm_row = html.Tr(instnm_row_cells)

    link_unit_display = []
    if selected_most_recent_name:
        filtered_data = get_filtered_data(selected_most_recent_name)
        if 'LinkUnit' in filtered_data.columns and not filtered_data['LinkUnit'].isnull().all():
            link_unit_value = filtered_data['LinkUnit'].dropna().unique()[0]
            associated_data = new_data[new_data['UNITID'] == link_unit_value]
            associated_names = associated_data['MostRecentName'].unique().tolist()

            link_unit_display.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
            link_unit_display.append(html.Span(f"{link_unit_value}", style={'background-color': 'yellow', 'font-weight': 'bold'}))
            if associated_names:
                link_unit_display.append(html.Span(", Associated Names: ", style={'font-weight': 'bold'}))
                for i, name in enumerate(associated_names):
                    link_unit_display.append(
                        html.A(
                            f"{name}", href="#", id={'type': 'merge-link', 'index': i},
                            **{'data-value': name},
                            style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                        )
                    )
                    link_unit_display.append(html.Span(", ", style={'font-weight': 'normal'}))
    if link_unit_display:
        link_unit_display = link_unit_display[:-1]

    table_rows = []
    max_rows = max(len(year_glabel_mapping.get(year, [])) for year in years)
    columns_content = {year: [] for year in years}

    for glabel in all_glabels_sorted:
        for year in years:
            if glabel in year_glabel_mapping.get(year, []):
                labels = get_unique_labels_for_year_glabel(year, glabel, new_df_with_unique_labels)
                cell_content = [
                    html.P(
                        label,
                        style={'background-color': 'yellow'} if label in new_data[
                            (new_data['MostRecentName'] == selected_most_recent_name) &
                            (new_data['YEAR'] == year)
                        ]['Label'].values else {}
                    )
                    for label in labels
                ]
                summary_style = {'background-color': '#FFD700', 'border-bottom': '1px solid black'} \
                    if any(label.style and label.style.get('background-color') == 'yellow' for label in cell_content) else {}
                columns_content[year].append(
                    html.Details(
                        [html.Summary(glabel, style=summary_style), html.Div(cell_content)],
                        open=any(label.style and label.style.get('background-color') == 'yellow' for label in cell_content)
                    )
                )
            else:
                columns_content[year].append('')

    for i in range(max_rows):
        row = [
            html.Td(
                columns_content[year][i] if i < len(columns_content[year]) else '',
                style={'vertical-align': 'top', 'border-bottom': '2px solid #ddd'}
            ) for year in years
        ]
        row.insert(0, html.Td(glabel, style={'font-weight':'bold'}))
        table_rows.append(html.Tr(row))

    return [table_header, instnm_row] + table_rows, html.Div(link_unit_display)

@app.callback(
    Output('most-recent-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'index': ALL}, 'data-value')]
)
def update_dropdown_on_click(n_clicks_list, data_values):
    if n_clicks_list is not None and any(n_clicks_list):
        clicked_index = n_clicks_list.index(max(n_clicks_list))
        return data_values[clicked_index]
    return dash.no_update

if __name__ == '__main__':
    app.run_server()
