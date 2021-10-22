package pod.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Neighbourhood implements DataSerializable {

    private String name;
    private int population;

    public Neighbourhood(String name, int population) {
        this.name = name;
        this.population = population;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(population);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        population = in.readInt();
    }


    public String getName() {
        return name;
    }

    public int getPopulation() {
        return population;
    }
}
