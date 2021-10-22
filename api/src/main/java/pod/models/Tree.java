package pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Tree implements DataSerializable {

    private String name, neighbour, street;

    public Tree() {}

    public Tree(String name, String neighbour, String street) {
        this.name = name;
        this.neighbour = neighbour;
        this.street = street;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(neighbour);
        out.writeUTF(street);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        neighbour = in.readUTF();
        street = in.readUTF();
    }


    public String getName() {
        return name;
    }

    public String getNeighbour() {
        return neighbour;
    }

    public String getStreet() {
        return street;
    }

}
