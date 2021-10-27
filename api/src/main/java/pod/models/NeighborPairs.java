package pod.models;

public class NeighborPairs implements Comparable<NeighborPairs>{
    private final int group;
    private final String neighborhoodA,neighborhoodB;

    public NeighborPairs(int group, String neighborhoodA, String neighborhoodB) {
        this.group = group;
        this.neighborhoodA = neighborhoodA;
        this.neighborhoodB = neighborhoodB;
    }

    public int getGroup() {
        return group;
    }

    public String getNeighborhoodA() {
        return neighborhoodA;
    }

    public String getNeighborhoodB() {
        return neighborhoodB;
    }

    @Override
    public int compareTo(NeighborPairs o) {
        if (group-o.group!=0)
            return group-o.group;
        else if (neighborhoodA.compareToIgnoreCase(o.neighborhoodA)!=0)
            return neighborhoodA.compareToIgnoreCase(o.neighborhoodA);
        else
            return neighborhoodB.compareToIgnoreCase(o.neighborhoodB);
    }
}
