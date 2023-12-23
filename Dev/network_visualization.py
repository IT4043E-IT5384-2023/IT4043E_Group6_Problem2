import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network
import pandas as pd

df = pd.read_csv('GroupProjectbyCentroid/tmp.csv')

st.set_page_config(layout="wide")


hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)


# Create a graph
G = nx.Graph()

for _, r in df.iterrows():
    if _ > 200:
        break
    node1 = r['user_address'][:8] + '...'
    node2 = f"cluster{r['prediction']}"
    length = r['distance_to_centroid']

    G.add_node(node1)
    G.add_node(node2)

    # Add weighted edges
    G.add_edge(node1, node2, weight=length)

def draw_network_with_weights(G):
    pos = nx.spring_layout(G)  # positions for all nodes
    
    # Setting up the figure size to try and make it full screen
    # plt.figure(figsize=(20, 20))
    
    # Drawing nodes and edges with their weights
    nx.draw_networkx_nodes(G, pos, node_size=700, node_color='lightblue')
    nx.draw_networkx_edges(G, pos, width=1.0, alpha=0.5)
    nx.draw_networkx_labels(G, pos, font_size=12)
    edge_labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    
    # Turn off the axis
    plt.axis('off')

    # Streamlit's pyplot function automatically fits the plot into the app's layout
    st.pyplot(plt)

def draw_interactive_network_with_weights(G):
    # Set up the network - adjust width and height to 100% to cover full screen
    nt = Network('100%', '100%', notebook=True)
    nt.from_nx(G)

    # Add custom options for a better-looking visualization
    # nt.set_options("""
    # var options = {
    #   "nodes": {
    #     "font": {
    #       "size": 12
    #     }
    #   },
    #   "edges": {
    #     "color": {
    #       "inherit": true
    #     },
    #     "smooth": false
    #   },
    #   "physics": {
    #     "forceAtlas2Based": {
    #       "springLength": 100
    #     },
    #     "minVelocity": 0.75,
    #     "solver": "forceAtlas2Based"
    #   }
    # }
    # """)
    

    # Generate and display the network
    nt.show('nx.html')
    HtmlFile = open('nx.html', 'r', encoding='utf-8')
    source_code = HtmlFile.read()
    
    # Use Streamlit components to render the HTML full-screen
    st.components.v1.html(source_code, height=20000, scrolling=False)


st.title('Users Cluster Visualization')

# Sidebar or main controls to interact with the network
# e.g., st.sidebar.checkbox('Show weights')

# Call the function to visualize the network
draw_interactive_network_with_weights(G)