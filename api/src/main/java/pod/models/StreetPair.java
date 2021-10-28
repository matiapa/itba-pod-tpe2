package pod.models;

import java.util.Objects;

public class StreetPair implements Comparable<StreetPair>{
    private final long group;
    private final String streetA, streetB;

    public StreetPair(long group, String street1, String street2) {
        this.group = group;
        this.streetA = street1.compareTo(street2) > 0 ? street1 : street2;
        this.streetB = street1.compareTo(street2) > 0 ? street2 : street1;
    }

    public long getGroup() {
        return group;
    }

    public String getStreetA() {
        return streetA;
    }

    public String getStreetB() {
        return streetB;
    }

    @Override
    public int compareTo(StreetPair o) {
        if (o.group-group!=0)
            return (int) (o.group-group);
        else if (o.streetA.compareToIgnoreCase(streetA)!=0)
            return o.streetA.compareToIgnoreCase(streetA);
        else
            return o.streetB.compareToIgnoreCase(streetB);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreetPair that = (StreetPair) o;
        return group == that.group && Objects.equals(streetA, that.streetA) && Objects.equals(streetB, that.streetB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, streetA, streetB);
    }
}
