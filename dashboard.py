from dash import Dash, html, dcc, callback, Output, Input
import dash_cytoscape as cyto
import state

app = Dash("LeMauvaisCoin")

app.layout = html.Div(children=[
    html.H1(children='Le Mauvais Coin'),

    html.Div(children='''
        Graphique Dashboard
    '''),

    dcc.Interval(
        id="interval",
        interval=5000
    ),

    cyto.Cytoscape(
        id='cytoscape-visualization',
        layout={
            'name': 'cose',
            'nodeRepulsion': 8000,
            'idealEdgeLength': 100,   
            'nodeOverlap': 20          
        },
        style={'width': '100%', 'height': '100vh'},
        elements=[],
        stylesheet=[
            {
                'selector': '[type = "USER"]',
                'style': {
                    'background-color': 'grey'
                }
            },
            {
                'selector': '[type = "SELLER"]',
                'style': {
                    'background-color': 'green'
                }
            },
            {
                'selector': '[type = "PRODUCT"]',
                'style': {
                    'background-color': 'blue'
                }
            },
            {
                'selector': 'edge[label = "ACHAT"]',
                'style': {
                    'line-color': 'red'
                }
            },
            {
                'selector': 'edge[label = "VOUT"]',
                'style': {
                    'line-color': 'orange'
                }
            },
            {
                'selector': 'edge[label = "AIME"]',
                'style': {
                    'line-color': 'yellow'
                }
            },
            {
                'selector': 'edge[label = "PROPOSE"]',
                'style': {
                    'line-color': 'brown'
                }
            }
        ]
    )    
])

@app.callback(
    Output("cytoscape-visualization", "elements"),
    Input("interval", "n_intervals")                
)
def update_visualization(n_intervals):
    nodes_elements = [
        {"data": {"id": v["id"], "label": v["type"], "type": v["type"]}}
        for v in state.all_vertices
    ]

    edges_elements = [
        {"data": {"source": e["src"], "target": e["dst"], "label": e["relation"]}}
        for e in state.all_edges
    ]

    return nodes_elements + edges_elements

    