from flask import jsonify
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd

# Load your data (replace with your actual filename)
# new_data = pd.read_csv('new_data.csv')
new_data = pd.read_parquet('new_data.parquet')

# Define the desired GLabel order
desired_order = [
    'Doctoral', "Master's", "Bachelor's", 'Associates',
    'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
]

def get_unique_labels_for_year_glabel(year, glabel, data_frame):
    """Return sorted unique labels for a given year and GLabel."""
    filtered_df = data_frame[(data_frame['YEAR'] == year) & (data_frame['GLabel'] == glabel)]
    return filtered_df.sort_values(by='NewLabel')['Label'].unique().tolist()

# Prepare data: remove duplicates and sort
unique_df = new_data.drop_duplicates(subset=['YEAR', 'GLabel', 'Label'])
unique_df['GLabel'] = pd.Categorical(unique_df['GLabel'], categories=desired_order, ordered=True)
unique_df = unique_df.sort_values(['YEAR', 'GLabel', 'SubCatLbl'])

all_glabels_sorted = unique_df['GLabel'].cat.categories
years = sorted(new_data['YEAR'].unique())
year_glabel_mapping = unique_df.groupby('YEAR')['GLabel'].unique().to_dict()

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server

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
    return jsonify(new_data.to_dict(orient="records"))

@app.callback(
    [Output('year-glabel-table', 'children'),
     Output('link-unit-display', 'children')],
    [Input('most-recent-name-dropdown', 'value')]
)
def update_table(selected_name):
    if not selected_name:
        return [], ""

    # Get Instnm for each year for the selected name
    instnm_by_year = new_data[new_data['MostRecentName'] == selected_name].groupby('YEAR')['Instnm'].first().to_dict()
    filtered_data = new_data[new_data['MostRecentName'] == selected_name]
    link_unit_exists = 'LinkUnit' in filtered_data.columns and not filtered_data['LinkUnit'].isnull().all()

    # Table header
    header_cells = [html.Th("Year")] + [html.Th(year) for year in years]
    if link_unit_exists:
        header_cells.append(html.Th("LinkUnit"))
    table_header = html.Tr(header_cells)

    # Instnm row
    instnm_cells = [html.Th("Instnm")] + [html.Th(instnm_by_year.get(year, 'N/A')) for year in years]
    if link_unit_exists:
        instnm_cells.append(html.Th(filtered_data['LinkUnit'].iloc[0]))
    instnm_row = html.Tr(instnm_cells)

    # LinkUnit display
    link_unit_display = []
    if link_unit_exists:
        link_unit_value = filtered_data['LinkUnit'].dropna().unique()[0]
        associated_names = new_data[new_data['UNITID'] == link_unit_value]['MostRecentName'].unique().tolist()
        link_unit_display += [
            html.Span("Merged Into: ", style={'font-weight': 'bold'}),
            html.Span(f"{link_unit_value}", style={'background-color': 'yellow', 'font-weight': 'bold'})
        ]
        if associated_names:
            link_unit_display.append(html.Span(", Associated Names: ", style={'font-weight': 'bold'}))
            for i, name in enumerate(associated_names):
                link_unit_display.append(
                    html.A(
                        name, href="#", id={'type': 'merge-link', 'index': i},
                        **{'data-value': name},
                        style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                    )
                )
                if i < len(associated_names) - 1:
                    link_unit_display.append(html.Span(", "))

    # Build table body
    columns_content = {year: [] for year in years}
    max_rows = 0
    for glabel in all_glabels_sorted:
        for year in years:
            if glabel in year_glabel_mapping.get(year, []):
                labels = get_unique_labels_for_year_glabel(year, glabel, unique_df)
                cell_content = [
                    html.P(
                        label,
                        style={'background-color': 'yellow'} if label in filtered_data[filtered_data['YEAR'] == year]['Label'].values else {}
                    )
                    for label in labels
                ]
                summary_style = {'background-color': '#FFD700', 'border-bottom': '1px solid black'} \
                    if any(p.props.get('style', {}).get('background-color') == 'yellow' for p in cell_content) else {}
                columns_content[year].append(
                    html.Details(
                        [html.Summary(glabel, style=summary_style), html.Div(cell_content)],
                        open=bool(summary_style)
                    )
                )
            else:
                columns_content[year].append('')
        max_rows = max(max_rows, len(columns_content[years[0]]))

    # Assemble table rows
    table_rows = []
    for i in range(max_rows):
        row = [html.Td(columns_content[year][i] if i < len(columns_content[year]) else '',
                       style={'vertical-align': 'top', 'border-bottom': '2px solid #ddd'})
               for year in years]
        row.insert(0, html.Td(''))
        table_rows.append(html.Tr(row))

    return [table_header, instnm_row] + table_rows, html.Div(link_unit_display)

@app.callback(
    Output('most-recent-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'index': ALL}, 'data-value')]
)
def update_dropdown_on_click(n_clicks_list, data_values):
    if n_clicks_list and any(n_clicks_list):
        clicked_index = n_clicks_list.index(max(filter(None, n_clicks_list)))
        return data_values[clicked_index]
    return dash.no_update

if __name__ == '__main__':
    app.run_server()
