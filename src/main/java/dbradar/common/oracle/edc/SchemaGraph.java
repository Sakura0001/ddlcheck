package dbradar.common.oracle.edc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaGraph<T> {

    private Map<T, Vertex<T>> vertices;
    private Map<Vertex<T>, List<Edge<T>>> adjacencyList;

    public SchemaGraph() {
        vertices = new HashMap<>();
        adjacencyList = new HashMap<>();
    }

    public Map<T, Vertex<T>> getVertices() {
        return vertices;
    }

    public List<Vertex<T>> getLeafVertices() {
        List<Vertex<T>> leafVertices = new ArrayList<>();
        for (Vertex<T> v : vertices.values()) {
            if (v.isLeaf()) {
                leafVertices.add(v);
            }
        }
        return leafVertices;
    }

    public Map<Vertex<T>, List<Edge<T>>> getAdjacencyList() {
        return adjacencyList;
    }

    public List<Edge<T>> getAdjacentEdges(Vertex<T> v) {
        return adjacencyList.get(v);
    }

    public Vertex<T> addVertex(T table) {
        Vertex<T> vertex = new Vertex<>(table);
        vertices.put(table, vertex);
        adjacencyList.put(vertex, new ArrayList<>());
        return vertex;
    }

    public Edge<T> addEdge(Vertex<T> from, Vertex<T> to, String edgeType) {
        Edge<T> edge = new Edge<>(from, to, edgeType);
        adjacencyList.get(to).add(edge);
        if (!edgeType.equals("FK")) {
            from.setLeaf(false);
        }
        return edge;
    }

    public static class Vertex<T> {
        private T table;
        private boolean isLeaf = true;

        public Vertex(T table) {
            this.table = table;
        }

        public T getTable() {
            return table;
        }

        public void setLeaf(boolean leaf) {
            isLeaf = leaf;
        }

        public boolean isLeaf() {
            return isLeaf;
        }
    }

    public static class Edge<T> {
        private Vertex<T> source;
        private Vertex<T> target;
        private String edgeType; // DDL type

        public Edge(Vertex<T> source, Vertex<T> target, String edgeType) {
            this.source = source;
            this.target = target;
            this.edgeType = edgeType;
        }

        public Vertex<T> getSource() {
            return source;
        }

        public Vertex<T> getTarget() {
            return target;
        }

        public String getEdgeType() {
            return edgeType;
        }
    }

}
