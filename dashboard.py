from dash import Dash, html, dcc, Output, Input
import dash_cytoscape as cyto
import plotly.graph_objects as go
import state

app = Dash("LeMauvaisCoin")

SIDEBAR_STYLE = {
    'width': '260px',
    'minWidth': '260px',
    'padding': '16px',
    'background': '#f5f5f5',
    'borderRight': '1px solid #ddd',
    'fontFamily': 'monospace',
    'fontSize': '13px',
    'overflowY': 'auto',
}

BADGE = {
    'USER':    {'background': '#607d8b', 'color': '#fff', 'padding': '2px 6px', 'borderRadius': '4px'},
    'SELLER':  {'background': '#4caf50', 'color': '#fff', 'padding': '2px 6px', 'borderRadius': '4px'},
    'PRODUCT': {'background': '#2196f3', 'color': '#fff', 'padding': '2px 6px', 'borderRadius': '4px'},
}

app.layout = html.Div(style={'display': 'flex', 'height': '100vh', 'flexDirection': 'column'}, children=[
    html.H1('Le Mauvais Coin', style={'margin': '12px 16px 4px', 'fontSize': '20px'}),

    dcc.Interval(id="interval", interval=5000),

    html.Div(style={'display': 'flex', 'flex': '1', 'overflow': 'hidden'}, children=[

        # --- Sidebar ---
        html.Div(style=SIDEBAR_STYLE, children=[

            html.H3('Nœud sélectionné', style={'marginTop': 0, 'fontSize': '13px', 'textTransform': 'uppercase', 'color': '#555'}),
            html.Div(id='node-info', children=[
                html.Span('Cliquer sur un nœud', style={'color': '#aaa'})
            ]),

            html.Hr(),

            html.H3('Top 5 PageRank', style={'fontSize': '13px', 'textTransform': 'uppercase', 'color': '#555'}),
            html.Div(id='pagerank-table'),

            html.Hr(),

            html.H3('Actions (fenêtre 1 min)', style={'fontSize': '13px', 'textTransform': 'uppercase', 'color': '#555'}),
            dcc.Graph(id='action-bar-chart', config={'displayModeBar': False}),

            html.Hr(),

            html.H3('Légende', style={'fontSize': '13px', 'textTransform': 'uppercase', 'color': '#555'}),
            html.Div([
                html.Div([html.Span('●', style={'color': '#607d8b', 'fontSize': '18px'}), ' Utilisateur']),
                html.Div([html.Span('●', style={'color': '#4caf50', 'fontSize': '18px'}), ' Vendeur']),
                html.Div([html.Span('●', style={'color': '#2196f3', 'fontSize': '18px'}), ' Produit']),
                html.Hr(style={'margin': '8px 0'}),
                html.Div([html.Span('─', style={'color': '#ffeb3b'}), ' AIME']),
                html.Div([html.Span('─', style={'color': '#ff9800'}), ' VOUT']),
                html.Div([html.Span('─', style={'color': '#e53935'}), ' ACHAT']),
                html.Div([html.Span('─', style={'color': '#795548'}), ' PROPOSE']),
            ]),
        ]),

        # --- Graph ---
        cyto.Cytoscape(
            id='cytoscape-visualization',
            layout={
                'name': 'cose',
                'nodeRepulsion': 8000,
                'idealEdgeLength': 100,
                'nodeOverlap': 20
            },
            style={'flex': '1', 'height': '100%'},
            elements=[],
            stylesheet=[
                {
                    'selector': 'node',
                    'style': {
                        'label': 'data(label)',
                        'font-size': '9px',
                        'text-valign': 'bottom',
                        'text-halign': 'center',
                        'text-margin-y': '4px',
                        'color': '#222',
                        'text-background-color': '#fff',
                        'text-background-opacity': 0.7,
                        'text-background-padding': '2px',
                        'width': 'mapData(degree, 0, 15, 22, 100)',
                        'height': 'mapData(degree, 0, 15, 22, 100)',
                    }
                },
                {
                    'selector': ':selected',
                    'style': {
                        'border-width': 3,
                        'border-color': '#ff5722',
                    }
                },
                {'selector': '[type = "USER"]',    'style': {'background-color': '#607d8b'}},
                {'selector': '[type = "SELLER"]',  'style': {'background-color': '#4caf50'}},
                {'selector': '[type = "PRODUCT"]', 'style': {'background-color': '#2196f3'}},
                {
                    'selector': 'edge',
                    'style': {
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'arrow-scale': 1.0,
                        'width': 1.5,
                        'label': 'data(label)',
                        'font-size': '6px',
                        'text-rotation': 'autorotate',
                        'text-background-color': '#fff',
                        'text-background-opacity': 0.75,
                        'text-background-padding': '1px',
                    }
                },
                {'selector': 'edge[label = "ACHAT"]',   'style': {'line-color': '#e53935', 'target-arrow-color': '#e53935'}},
                {'selector': 'edge[label = "VOUT"]',    'style': {'line-color': '#ff9800', 'target-arrow-color': '#ff9800'}},
                {'selector': 'edge[label = "AIME"]',    'style': {'line-color': '#ffeb3b', 'target-arrow-color': '#ffeb3b'}},
                {'selector': 'edge[label = "PROPOSE"]', 'style': {'line-color': '#795548', 'target-arrow-color': '#795548'}},
            ]
        ),
    ]),
])


@app.callback(
    Output("cytoscape-visualization", "elements"),
    Input("interval", "n_intervals")
)
def update_visualization(n_intervals):
    nodes_elements = []
    for v in state.all_vertices:
        m = state.graph_metrics.get(v["id"], {})
        degree = m.get("inDegree", 0) + m.get("outDegree", 0)
        nodes_elements.append({
            "data": {
                "id": v["id"],
                "label": v["id"],
                "type": v["type"],
                "degree": degree,
                "inDegree": m.get("inDegree", 0),
                "outDegree": m.get("outDegree", 0),
                "pagerank": m.get("pagerank", 0.0),
            }
        })

    edges_elements = [
        {"data": {"source": e["src"], "target": e["dst"], "label": e["relation"]}}
        for e in state.all_edges
    ]

    return nodes_elements + edges_elements


@app.callback(
    Output("node-info", "children"),
    Input("cytoscape-visualization", "tapNodeData")
)
def display_node_info(data):
    if not data:
        return html.Span('Cliquer sur un nœud', style={'color': '#aaa'})

    node_type = data.get("type", "?")
    badge_style = BADGE.get(node_type, {})

    return html.Div([
        html.Div([html.Span(node_type, style=badge_style), f'  {data["id"]}'], style={'marginBottom': '8px', 'fontWeight': 'bold'}),
        html.Div(f'Degré entrant :  {data.get("inDegree", 0)}'),
        html.Div(f'Degré sortant :  {data.get("outDegree", 0)}'),
        html.Div(f'Degré total :    {data.get("degree", 0)}'),
        html.Div(f'PageRank :       {data.get("pagerank", 0.0):.4f}'),
    ])


@app.callback(
    Output("action-bar-chart", "figure"),
    Input("interval", "n_intervals")
)
def update_action_chart(_):
    counts = state.action_counts
    actions = ["AIME", "VOUT", "ACHAT"]
    colors = {"AIME": "#ffeb3b", "VOUT": "#ff9800", "ACHAT": "#e53935"}
    fig = go.Figure(go.Bar(
        x=actions,
        y=[counts.get(a, 0) for a in actions],
        marker_color=[colors[a] for a in actions],
    ))
    fig.update_layout(
        height=150,
        margin=dict(l=10, r=10, t=10, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=10),
        yaxis=dict(gridcolor="#ddd", rangemode="tozero"),
    )
    return fig


@app.callback(
    Output("pagerank-table", "children"),
    Input("interval", "n_intervals")
)
def update_pagerank_table(_):
    if not state.graph_metrics:
        return html.Span('En attente de données...', style={'color': '#aaa'})

    top5 = sorted(state.graph_metrics.items(), key=lambda x: x[1].get("pagerank", 0), reverse=True)[:5]

    rows = []
    for rank, (node_id, m) in enumerate(top5, 1):
        node_type = next((v["type"] for v in state.all_vertices if v["id"] == node_id), "?")
        badge_style = {**BADGE.get(node_type, {}), 'fontSize': '10px'}
        rows.append(html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '4px'}, children=[
            html.Span([f'{rank}. ', html.Span(node_type[0], style=badge_style), f' {node_id}']),
            html.Span(f'{m.get("pagerank", 0):.3f}', style={'color': '#888'}),
        ]))

    return rows
