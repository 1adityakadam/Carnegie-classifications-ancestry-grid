import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd

# Load data for dropdown and grouping
dropdown_df = pd.read_parquet('updated_data.parquet', columns=['current_name', 'inst_name', 'unit_id'])
dropdown_df['display_current_name'] = dropdown_df['current_name'] + ' {' + dropdown_df['unit_id'].astype(str) + '}'
dropdown_df['display_inst_name'] = dropdown_df['inst_name'] + ' {' + dropdown_df['unit_id'].astype(str) + '}'

# Build dropdown options: group by current_name and unit_id, collect all unique past names
dropdown_options = []
for (current_name, unit_id), group in dropdown_df.groupby(['current_name', 'unit_id']):
    # Collect all unique, non-null, non-N/A past names for this current_name/unit_id pair
    past_names = sorted(
        set(
            name for name in group['inst_name'].unique()
            if pd.notna(name) and name != "N/A" and name != current_name
        )
    )
    
    # Create display label with unit_id
    if past_names:
        label = f"{current_name} {{{unit_id}}} (Earlier: {', '.join(past_names)})"
    else:
        label = f"{current_name} {{{unit_id}}}"
    
    # Use current_name|||unit_id as the value for internal lookup
    dropdown_options.append({'label': label, 'value': f"{current_name}|||{unit_id}"})

# Sort by label
dropdown_options = sorted(dropdown_options, key=lambda x: x['label'])

# Maintain original grouping logic with sorted current_names
unique_names = [opt['value'].split('|||')[0] for opt in dropdown_options]  # Extract current_name for grouping
name_to_group = {name: idx // 10 for idx, name in enumerate(sorted(set(unique_names)))}

# Load the main data
new_data = pd.read_parquet('updated_data.parquet')

# Dash App Layout
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    [ dcc.Dropdown(
            id='current-name-dropdown',
            options=dropdown_options,
            placeholder="Select Institution Name"
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
                "height": "100vh",
                "borderRadius": "8px",
                "padding": "18px",
                "margin": "5px",
                "maxWidth": "calc(100%-5px)",
                "width": "calc(100%-5px)",
                "overflowY": "auto"
            }
        ),
    ],
    style={"height": "100vh"
          }
)

# Helper function & desired order
desired_order = [
    'Doctoral',
    "Master's",
    "Bachelor's",
    'Associates',
    'Bacc/Assoc',
    'SF: 2Yr',
    'SF: 4Yr',
    'Tribal/Oth',
    'Not in'
]

#this code is for alphabetically sorting the class_status values
# def get_unique_labels_for_year_degree_label(year, degree_label):
#     filtered_df = new_data[
#         (new_data['year'] == year) &
#         (new_data['degree_label'] == degree_label)
#     ]
#     unique_labels = filtered_df['class_status'].unique().tolist()
#     unique_labels = sorted(
#         unique_labels,
#         key=lambda s: (s[0].lower(), len(s))
#     )
#     return unique_labels

#this code is for sorting the class_status values by const_cat_value (NewLabels)
def get_unique_labels_for_year_degree_label(year, degree_label):
    filtered_df = new_data[
        (new_data['year'] == year) &
        (new_data['degree_label'] == degree_label)
    ]
    # Drop duplicates to get unique (class_status, const_cat_value) pairs
    unique_pairs = filtered_df[['class_status', 'const_cat_value']].drop_duplicates()
    # Sort by const_cat_value
    unique_pairs = unique_pairs.sort_values('const_cat_value')
    # Return the class_status values in the sorted order
    return unique_pairs['class_status'].tolist()

# Callback to update table and merged-into display
@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_table(selected_current_name):
    if not selected_current_name:
        return [], ""

    # Parse the dropdown value to get current_name and unit_id
    selected_name, selected_unit_id = selected_current_name.split('|||')
    
    group_id = name_to_group[selected_name]
    group_data = pd.read_parquet(
        f'updated_data_grouped/group_id={group_id}/'
    )
    filtered_data = group_data[
        (group_data['current_name'] == selected_name) & 
        (group_data['unit_id'] == int(selected_unit_id))
    ]

    years = sorted(new_data['year'].unique())
    inst_name_by_year = (
        new_data[
            (new_data['current_name'] == selected_name) & 
            (new_data['unit_id'] == int(selected_unit_id))
        ]
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
    left_col_width = "40px"
    col_width = f"{100/n_cols:.2f}%"
    # f"calc((100% - {left_col_width}) / {n_cols - 1})"

    # Table header
    table_header_cells = [html.Th("Year", style={
        'width': left_col_width,
        'minWidth': left_col_width,
        'maxWidth': left_col_width,
        'whiteSpace': 'normal',
        'wordBreak': 'break-word',
        'padding': '8px',
        'textAlign': 'left'
    })] + [
        html.Th(year, style={
            'width': col_width,
            'minWidth': '70px',
            'maxWidth': col_width,
            'whiteSpace': 'normal',
            'wordBreak': 'break-word',
            'padding': '8px',
            'textAlign': 'left'
        }) for year in years
    ]
    table_header = html.Tr(table_header_cells)

    # Institution name row
    inst_name_row_cells = [html.Th("Inst. Name", style={
        'fontWeight': 'bold',
        'width': left_col_width,
        'minWidth': left_col_width,
        'maxWidth': left_col_width,
        'whiteSpace': 'normal',
        'wordBreak': 'break-word',
        'padding': '8px',
        'textAlign': 'left'
    })] + [
        html.Th(inst_name_by_year.get(year, 'N/A'), style={
            'width': col_width,
            'minWidth': '70px',
            'maxWidth': col_width,
            'whiteSpace': 'normal',
            'wordBreak': 'break-word',
            'padding': '8px',
            'textAlign': 'left'
        }) for year in years
    ]
    
    inst_name_row = html.Tr(inst_name_row_cells, style={
            'backgroundColor': 'rgba(63, 119, 225,0.3)'
    })

    # Build table rows
    table_rows = []
    for degree_label in desired_order:
        cells = [
            html.Td("", style={
                'width': left_col_width,
                'minWidth': left_col_width,
                'maxWidth': left_col_width,
                'verticalAlign': 'top',
                'textAlign': 'left',
                'padding': '8px',
                'whiteSpace': 'normal',
                'wordBreak': 'break-word',
                'borderBottom': '2px solid #ddd',
            })
        ]
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
        table_rows.append(html.Tr(cells))

    # Merged into and absorbed display logic - Inline format like reference
    display_elements = []
    
    # Check for merged_into information
    if 'merged_into_id' in filtered_data.columns and not filtered_data['merged_into_id'].isnull().all():
        # Filter out -1 values which represent closed/non-existent institutions
        valid_merged_into = filtered_data['merged_into_id'].dropna()
        valid_merged_into = valid_merged_into[valid_merged_into != -1]
        
        if not valid_merged_into.empty:
            merged_into_value = valid_merged_into.unique()[0]
            associated_data = new_data[new_data['unit_id'] == merged_into_value]
            associated_names = [
                name for name in associated_data['current_name'].unique().tolist()
                if pd.notnull(name) and name != "None"
            ]
            
            # Find the year when the institution was merged
            merge_year = filtered_data[
                (filtered_data['merged_into_id'].notna()) & 
                (filtered_data['merged_into_id'] != -1)
            ]['year'].min()
            
            if associated_names:
                display_elements.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
                for i, name in enumerate(associated_names):
                    # Create proper dropdown value using current_name and unit_id from associated_data
                    uid = associated_data[associated_data['current_name'] == name]['unit_id'].iloc[0]
                    dropdown_value = f"{name}|||{uid}"
                    
                    display_elements.append(
                        html.A(
                            name, href="#",
                            id={'type': 'merge-link', 'unit_id': str(merged_into_value), 'index': i},
                            **{'data-value': dropdown_value},
                            style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                        )
                    )
                    if i < len(associated_names) - 1:
                        display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))
                        
                display_elements.append(html.Span(f" ({merge_year})", style={'font-weight': 'bold'}))
    
    # Check for absorbed institutions (merged_from)
    if 'unit_id' in filtered_data.columns:
        current_unit_id = filtered_data['unit_id'].iloc[0]
        merged_from_records = new_data[new_data['merged_into_id'] == current_unit_id]
        if not merged_from_records.empty:
            if display_elements:
                display_elements.append(html.Br())
                display_elements.append(html.Br())
            merged_from_info = merged_from_records[['unit_id', 'current_name', 'year']].drop_duplicates()
            for i, (idx, row) in enumerate(merged_from_info.iterrows()):
                unit_id = row['unit_id']
                absorption_year = row['year']
                inst_names_data = new_data[new_data['unit_id'] == unit_id]
                if not inst_names_data.empty and 'inst_name' in inst_names_data.columns:
                    inst_names = [
                        name for name in inst_names_data['inst_name'].unique()
                        if pd.notnull(name) and name != "None"
                    ]
                    if inst_names:
                        display_elements.append(html.Span("Absorbed: ", style={'font-weight': 'bold'}))
                        # Create proper dropdown value using current_name and unit_id
                        dropdown_value = f"{row['current_name']}|||{unit_id}"
                        
                        for j, name in enumerate(inst_names):
                            display_elements.append(
                                html.A(
                                    name, href="#",
                                    id={'type': 'merged-from-link', 'unit_id': str(unit_id), 'index': j},
                                    **{'data-value': dropdown_value},
                                    style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                                )
                            )
                            if j < len(inst_names) - 1:
                                display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))
                                
                        display_elements.append(html.Span(f" ({absorption_year})", style={'font-weight': 'bold'}))
                        
                if i < len(merged_from_info) - 1:
                    display_elements.append(html.Br())
                    display_elements.append(html.Br())

    # Combine all display elements
    merge_display = html.Div(display_elements) if display_elements else html.Div()

    return [table_header, inst_name_row] + table_rows, merge_display

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
