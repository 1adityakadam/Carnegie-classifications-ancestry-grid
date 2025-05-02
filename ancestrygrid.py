from flask import Response
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
from flask_caching import Cache
import orjson

# Load data with proper categorical conversion
new_data = pd.read_parquet('new_data.parquet', engine='pyarrow')
categorical_cols = ['GLabel', 'Label', 'SubCatLbl']
new_data[categorical_cols] = new_data[categorical_cols].astype('category')

desired_order = [
    'Doctoral', "Master's", "Bachelor's", 'Associates',
    'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
]

def get_unique_labels_for_year_glabel(year, glabel, data_frame):
    filtered_df = data_frame[(data_frame['YEAR'] == year) & (data_frame['GLabel'] == glabel)]
    return filtered_df.sort_values(by='NewLabel')['Label'].unique().tolist()

app = dash.Dash(__name__)
server = app.server

cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory',
    'CACHE_THRESHOLD': 100
})

# Cache preprocessing but keep original data intact
@cache.memoize(timeout=3600)
def prepare_data():
    df = new_data.drop_duplicates(subset=['YEAR', 'GLabel', 'Label'])
    df['GLabel'] = pd.Categorical(df['GLabel'], categories=desired_order, ordered=True)
    return df.sort_values(['GLabel', 'SubCatLbl'])

preprocessed_data = prepare_data()
year_glabel_mapping = preprocessed_data.groupby('YEAR')['GLabel'].unique().to_dict()
all_glabels_sorted = preprocessed_data['GLabel'].unique().tolist()

# Layout improvements
app.layout = html.Div([
    dcc.Dropdown(
        id='most-recent-name-dropdown',
        options=[{'label': name, 'value': name} 
                for name in new_data['MostRecentName'].unique()],  # Use original data for dropdown
        placeholder="Select a MostRecentName",
        style={'minWidth': '400px', 'marginBottom': '20px'}
    ),
    html.Div(id='link-unit-display', style={'margin': '10px 0'}),
    html.Table(
        id='year-glabel-table',
        style={'width': '100%', 'borderCollapse': 'collapse'}
    )
], style={'padding': '20px', 'fontFamily': 'Arial'})

@app.server.route('/get_data')
def get_data():
    return Response(
        orjson.dumps(new_data.to_dict(orient="records")),
        mimetype='application/json'
    )

@cache.memoize(timeout=300)
def get_filtered_data(selected_name):
    return new_data[new_data['MostRecentName'] == selected_name]

@app.callback(
    [Output('year-glabel-table', 'children'),
     Output('link-unit-display', 'children')],
    [Input('most-recent-name-dropdown', 'value')],
    prevent_initial_call=True
)
def update_table(selected_name):
    if not selected_name:
        return [], ""
        
    filtered_data = get_filtered_data(selected_name)
    years = sorted(new_data['YEAR'].unique())
    
    # Header row
    header = [html.Th("GLabel", style={'padding': '8px', 'borderBottom': '2px solid #ddd'})] + [
        html.Th(str(year), style={'padding': '8px', 'borderBottom': '2px solid #ddd'}) 
        for year in years
    ]
    
    # Institution name row
    instnm_row = [html.Th("Institution", style={'padding': '8px'})] + [
        html.Td(filtered_data[filtered_data['YEAR'] == year]['Instnm'].values[0] 
               if not filtered_data[filtered_data['YEAR'] == year].empty else 'N/A',
               style={'padding': '8px'}) 
        for year in years
    ]
    
    # Table body
    rows = []
    for glabel in all_glabels_sorted:
        cells = [html.Td(glabel, style={
            'fontWeight': 'bold', 
            'padding': '8px',
            'borderBottom': '1px solid #ddd'
        })]
        
        for year in years:
            if glabel in year_glabel_mapping.get(year, []):
                labels = get_unique_labels_for_year_glabel(year, glabel, preprocessed_data)
                highlight_style = {
                    'backgroundColor': 'yellow' if any(
                        label in filtered_data['Label'].values 
                        for label in labels
                    ) else None
                }
                cells.append(html.Td(
                    [html.Div(label, style=highlight_style) for label in labels],
                    style={'padding': '8px', 'verticalAlign': 'top'}
                ))
            else:
                cells.append(html.Td('-', style={'padding': '8px'}))
        
        rows.append(html.Tr(cells))
    
    # Link unit display
    link_unit_display = []
    if 'LinkUnit' in filtered_data.columns and not filtered_data['LinkUnit'].isnull().all():
        link_unit_value = filtered_data['LinkUnit'].iloc[0]
        associated_data = new_data[new_data['UNITID'] == link_unit_value]
        associated_names = associated_data['MostRecentName'].unique().tolist()
        
        link_unit_display.append(html.Span("Merged Into: ", style={'fontWeight': 'bold'}))
        link_unit_display.append(html.Span(str(link_unit_value), style={
            'backgroundColor': 'yellow',
            'fontWeight': 'bold',
            'padding': '2px 5px',
            'borderRadius': '3px'
        }))
        
        if associated_names:
            link_unit_display.append(html.Span(" Associated Names: ", style={'fontWeight': 'bold'}))
            links = []
            for name in associated_names:
                links.append(html.A(
                    name,
                    href="#",
                    style={
                        'color': 'blue',
                        'textDecoration': 'underline',
                        'cursor': 'pointer',
                        'marginRight': '5px'
                    },
                    id={'type': 'merge-link', 'index': name}
                ))
            link_unit_display.extend(links)

    return [html.Tr(header), html.Tr(instnm_row)] + rows, html.Div(link_unit_display)

@app.callback(
    Output('most-recent-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'index': ALL}, 'id')]
)
def update_dropdown(n_clicks_list, ids):
    if not n_clicks_list or all(click is None for click in n_clicks_list):
        return dash.no_update
    
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    return orjson.loads(triggered_id)['index']

if __name__ == '__main__':
    app.run_server(debug=True)
