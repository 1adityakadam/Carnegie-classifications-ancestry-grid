import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd


# Load datasets

current_name_col = pd.read_parquet(
    'updated_data.parquet',
    columns=['current_name']
)
all_current_names = current_name_col['current_name'].unique().tolist()

new_data = pd.read_parquet('updated_data.parquet')

unique_names = sorted(all_current_names)
name_to_group = {name: idx // 10 for idx, name in enumerate(unique_names)}


# Dash App Layout

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    [
        dcc.Dropdown(
            id='current-name-dropdown',
            options=[{'label': name, 'value': name} for name in sorted(all_current_names)],
            placeholder="Select a Current Name"
        ),
        
        html.Div(style={'height': '20px'}),
        html.Div(
            [
                html.Div(id='merged-into-display'),
                html.Div(style={'height': '20px'}),
                html.Div(
                    html.Table(
                        id='year-degree-label-table',
                        style={
                            'width': '100%',
                            'tableLayout': 'fixed',
                            'borderCollapse': 'collapse'
                        }
                    ),
                    id='table-container',
                    style={
                        'width': '100%',
                        'maxWidth': '100%',
                    }
                ),
            ],
            style={
                "height": "90vh",
                "borderRadius": "8px",
                "padding": "18px",
                "margin": "5px",
                "maxWidth": "calc(100%-5px)",
                "width": "calc(100%-5px)",
                "boxShadow": "0 2px 8px #d0d6e6",
                "overflowY": "auto"
            }
        ),
    ],
    style={"height": "100vh"}
)

# Helper function & desired order

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

def get_unique_labels_for_year_degree_label(year, degree_label):
    filtered_df = new_data[
        (new_data['year'] == year) &
        (new_data['degree_label'] == degree_label)
    ]
    unique_labels = filtered_df['class_status'].unique().tolist()
    unique_labels = sorted(
        unique_labels,
        key=lambda s: (s[0].lower(), len(s))
    )
    return unique_labels

# Callback to update table and merged-into display

@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_table(selected_current_name):
    if not selected_current_name:
        return [], ""

    group_id = name_to_group[selected_current_name]
    group_data = pd.read_parquet(
        f'updated_data_grouped/group_id={group_id}/'
    )
    filtered_data = group_data[group_data['current_name'] == selected_current_name]

    years = sorted(new_data['year'].unique())
    inst_name_by_year = (
        new_data[new_data['current_name'] == selected_current_name]
        .groupby('year')['inst_name']
        .first()
        .to_dict()
    )

    merged_into_exists = (
        'merged_into_id' in filtered_data.columns and
        not filtered_data['merged_into_id'].isnull().all()
    )

    # Calculate number of columns (degree label + years [+ merged_into])
    n_cols = 1 + len(years) + (1 if merged_into_exists else 0)
    col_width = f"{100/n_cols:.2f}%"

    # Table header
    table_header_cells = [html.Th("Year", style={
        'width': col_width,
        'minWidth': '70px',
        'maxWidth': col_width,
        'whiteSpace': 'normal',
        'wordBreak': 'break-word',
        'padding': '8px',
        'textAlign': 'left',
         #---------------------------------------May 22, 2025-------
        'backgroundColor': '#e8f1ff',
         #---------------------------------------May 22, 2025-------
    })] + [
        html.Th(year, style={
            'width': col_width,
            'minWidth': '70px',
            'maxWidth': col_width,
            'whiteSpace': 'normal',
            'wordBreak': 'break-word',
            'padding': '8px',
            'textAlign': 'left',
             #---------------------------------------May 22, 2025-------
            'backgroundColor': '#e8f1ff',
             #---------------------------------------May 22, 2025-------
        }) for year in years
    ]
    
    table_header = html.Tr(table_header_cells)

    # Institution name row
    inst_name_row_cells = [html.Th("Institution Name", style={
        'fontWeight': 'bold',
        'width': col_width,
        'minWidth': '70px',
        'maxWidth': col_width,
        'whiteSpace': 'normal',
        'wordBreak': 'break-word',
        'padding': '8px',
        'textAlign': 'left',
        
         #---------------------------------------May 22, 2025-------
        'backgroundColor': '#e6f2ff',
         #---------------------------------------May 22, 2025-------
        
    })] + [
        html.Th(inst_name_by_year.get(year, 'N/A'), style={
            'width': col_width,
            'minWidth': '70px',
            'maxWidth': col_width,
            'whiteSpace': 'normal',
            'wordBreak': 'break-word',
            'padding': '8px',
            'textAlign': 'left',
            #---------------------------------------May 22, 2025-------
            'backgroundColor': '#e6f2ff',
            #---------------------------------------May 22, 2025-------
        }) for year in years
    ]
    
    inst_name_row = html.Tr(inst_name_row_cells)

    # Build table rows
    table_rows = []
    for degree_label in desired_order:
        #---------------------------------------May 22, 2025-------
        cells = [
            html.Td(
            #     # degree_label, 
            #     style={
            #     'verticalAlign': 'top',
            #     'fontWeight': 'bold',
            #     # 'backgroundColor': '#e6f2ff',
            #     'width': '20px',
            #     'minWidth': '10px',
            #     'maxWidth': '20px',
            #     'whiteSpace': 'normal',
            #     'wordBreak': 'break-word',
            #     'textAlign': 'left',
            #     'padding': '8px'
            # }
            )
        ]
        #---------------------------------------May 22, 2025-------
        for year in years:
            all_statuses = get_unique_labels_for_year_degree_label(year, degree_label)
            inst_statuses = filtered_data[
                (filtered_data['year'] == year) &
                (filtered_data['degree_label'] == degree_label)
            ]['class_status'].unique().tolist()

            if all_statuses:
                status_elements = []
                for status in all_statuses:
                    style = (
                        {'background-color': 'lightblue', 'margin': '2px 0', 'padding': '2px 8px', 'borderRadius': '4px'}
                        if status in inst_statuses else
                        {'margin': '2px 0', 'padding': '2px 8px'}
                    )
                    status_elements.append(html.P(status, style=style))

                highlighted = bool(inst_statuses)
                summary_style = {
                    'width': '100%',
                    'minWidth': '0',
                    'boxSizing': 'border-box',
                    'padding': '8px 12px',
                    'fontWeight': 'bold',
                    'lineHeight': '1.2',
                    'cursor': 'pointer',
                    'whiteSpace': 'normal',
                    'wordBreak': 'break-word',
                    'backgroundColor': 'skyblue' if highlighted else 'inherit',
                    'borderRadius': '4px' if highlighted else '0'
                }

                cell_content = html.Details(
                    [
                        html.Summary(
                            f"{degree_label}",
                            style=summary_style
                        ),
                        html.Div(status_elements)
                    ],
                    open=highlighted,
                    style={
                        'width': '100%',
                    }
                )
            else:
                cell_content = html.Div("-", style={'height': '40px'})

            cells.append(html.Td(
                cell_content,
                style={
                    'width': col_width,
                    'minWidth': '70px',
                    'maxWidth': col_width,
                    'verticalAlign': 'top',
                    'textAlign': 'left',
                    'padding': '8px',
                    'whiteSpace': 'normal',
                    'wordBreak': 'break-word',
                    'borderBottom': '2px solid #ddd'
                }
            ))

        # if merged_into_exists:
        #     cells.append(html.Td("", style={
        #         'width': col_width,
        #         'minWidth': '70px',
        #         'maxWidth': col_width,
        #         'whiteSpace': 'normal',
        #         'wordBreak': 'break-word',
        #         'padding': '8px',
        #         'textAlign': 'left'
        #     }))

        table_rows.append(html.Tr(cells))

    # Merge logic fixes (using new_data from updated_data.parquet)
    display_elements = []
    if 'merged_into_id' in filtered_data.columns and not filtered_data['merged_into_id'].isnull().all():
        merged_into_value = filtered_data['merged_into_id'].dropna().unique()[0]
        associated_data = new_data[new_data['unit_id'] == merged_into_value]
        associated_names = [
            name for name in associated_data['current_name'].unique().tolist()
            if pd.notnull(name) and name != "None"
        ]
        
        if associated_names:
            display_elements.append(html.Span("Absorbed: ", style={'font-weight': 'bold'}))
            for i, name in enumerate(associated_names):
                display_elements.append(
                    html.A(
                        f"{name}", href="#",
                        id={'type': 'merge-link', 'unit_id': str(merged_into_value), 'index': i},
                        **{'data-value': name},
                        style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                    )
                )
                if i < len(associated_names) - 1:
                    display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))
    
    if 'unit_id' in filtered_data.columns:
        current_unit_id = filtered_data['unit_id'].iloc[0]
        merged_from_records = new_data[new_data['merged_into_id'] == current_unit_id]
        if not merged_from_records.empty:
            if display_elements:
                display_elements.append(html.Br())
                display_elements.append(html.Br())
                
            merged_from_info = merged_from_records[['unit_id', 'current_name']].drop_duplicates()
            for i, (idx, row) in enumerate(merged_from_info.iterrows()):
                unit_id = row['unit_id']
                
                inst_names_data = new_data[new_data['unit_id'] == unit_id]
                if not inst_names_data.empty and 'inst_name' in inst_names_data.columns:
                    inst_names = [
                        name for name in inst_names_data['inst_name'].unique()
                        if pd.notnull(name) and name != "None"
                    ]
                    if inst_names:
                        # ----------------------------------------------------------------------------May 22, 2025---------------------------------
                        display_elements.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
                        # -------------------------------------------------------------------------------------------------------------------------
                        for j, name in enumerate(inst_names):
                            display_elements.append(
                                html.A(
                                    f"{name}", href="#",
                                    id={'type': 'merged-from-link', 'unit_id': str(unit_id), 'index': j},
                                    **{'data-value': row['current_name']},
                                    style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                                )
                            )
                            if j < len(inst_names) - 1:
                                display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))
                if i < len(merged_from_info) - 1:
                    display_elements.append(html.Br())
                    display_elements.append(html.Br())

    return [table_header, inst_name_row] + table_rows, html.Div(display_elements)

# Callback for dropdown link clicks

@app.callback(
    Output('current-name-dropdown', 'value'),
    [
        Input({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks'),
        Input({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks')
    ],
    [
        State({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'data-value'),
        State({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'data-value')
    ]
)
def update_dropdown_on_click(merge_into_clicks, merge_from_clicks, merge_into_values, merge_from_values):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    trigger_prop_id = ctx.triggered[0]['prop_id']
    if 'merge-link' in trigger_prop_id:
        for i, clicks in enumerate(merge_into_clicks):
            if clicks and i < len(merge_into_values):
                return merge_into_values[i]
    elif 'merged-from-link' in trigger_prop_id:
        for i, clicks in enumerate(merge_from_clicks):
            if clicks and i < len(merge_from_values):
                return merge_from_values[i]
    return dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
