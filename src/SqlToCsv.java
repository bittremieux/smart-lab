import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import inspector.imondb.io.IMonDBManagerFactory;
import inspector.imondb.io.IMonDBReader;
import inspector.imondb.model.*;

import javax.persistence.EntityManagerFactory;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SqlToCsv {

    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        // connect to database iMonDB with user: imondb and password: imondb
        String database = "iMonDB";
        EntityManagerFactory emf = IMonDBManagerFactory.createMySQLFactory("localhost", "3306", database, "imondb", "imondb");
        IMonDBReader reader = new IMonDBReader(emf);

        List<String> instrumentNames = reader.getFromCustomQuery("SELECT inst.name FROM Instrument inst", String.class);

        // export instrument parameters for all instruments
        for(String instrumentName : instrumentNames) {
            // get all runs for this instrument
            List<String> runNames = reader.getFromCustomQuery("SELECT run.name FROM Run run WHERE run.instrument.name = :name",
                    String.class, ImmutableMap.of("name", instrumentName));

            if(runNames.size() > 0) {
                // get all numeric "statuslog" metrics for this instrument
                // store in a table of the form run - accession - value
                Table<Run, String, Value> values = HashBasedTable.create();
                for(String runName : runNames) {
                    Run run = reader.getRun(runName, instrumentName);
                    for(Iterator<Value> valueIterator = run.getValueIterator(); valueIterator.hasNext(); ) {
                        Value value = valueIterator.next();
                        if(value.getDefiningProperty().getType().equals("statuslog") && value.getDefiningProperty().getNumeric()) {
                            values.put(run, value.getDefiningProperty().getAccession(), value);
                        }
                    }
                }
                List<String> sortedProperties = new ArrayList<>(values.columnKeySet());
                Collections.sort(sortedProperties);

                // export the metrics
                try {
                    PrintWriter writer = new PrintWriter(database + "_" + instrumentName + ".txt");
//                    writer.print("Filename\tStorageName\tDate\t");
                    writer.print("Date\t");
                    // write the header
                    for(int property_idx = 0; property_idx < sortedProperties.size(); property_idx++) {
                        String property = sortedProperties.get(property_idx);
                        writer.print(property.replace(' ', '_') + "_Min" + "\t");
                        writer.print(property.replace(' ', '_') + "_Max" + "\t");
                        writer.print(property.replace(' ', '_') + "_Mean" + "\t");
                        writer.print(property.replace(' ', '_') + "_Median" + "\t");
                        writer.print(property.replace(' ', '_') + "_Q1" + "\t");
                        writer.print(property.replace(' ', '_') + "_Q3" + "\t");
                        writer.print(property.replace(' ', '_') + "_SD");
                        if(property_idx < sortedProperties.size() - 1) {
                            writer.print("\t");
                        }
                    }
                    writer.println();

                    // write the values for each run (in chronological order)
                    List<Run> sortedRuns = new ArrayList<>(values.rowKeySet());
                    Collections.sort(sortedRuns, (o1, o2) -> o1.getSampleDate().compareTo(o2.getSampleDate()));
                    for(int run_idx = 0; run_idx < sortedRuns.size(); run_idx++) {
                        Run run = sortedRuns.get(run_idx);
//                        writer.print(run.getName() + "\t");
//                        writer.print(run.getStorageName() + "\t");
                        writer.print(sdf.format(run.getSampleDate()) + "\t");
                        for(int property_idx = 0; property_idx < sortedProperties.size(); property_idx++) {
                            String property = sortedProperties.get(property_idx);
                            Value value = values.get(run, property);
                            if(value != null) {
                                writer.print(value.getMin() + "\t");
                                writer.print(value.getMax() + "\t");
                                writer.print(value.getMean() + "\t");
                                writer.print(value.getMedian() + "\t");
                                writer.print(value.getQ1() + "\t");
                                writer.print(value.getQ3() + "\t");
                                writer.print(value.getSd());
                            } else {
                                writer.print("\t\t\t\t\t\t");
                            }
                            if(property_idx < sortedProperties.size() - 1) {
                                writer.print("\t");
                            }
                        }
                        if(run_idx < sortedRuns.size() - 1) {
                            writer.println();
                        }
                    }
                    writer.close();
                } catch(FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        // export temperature measurements for applicable instruments
        for(String instrumentName : instrumentNames) {
            Instrument instrument = reader.getInstrument(instrumentName, true, false);
            List<Event> events = new ArrayList<>();
            for(Iterator<Event> it = instrument.getEventIterator(); it.hasNext(); ) {
                Event event = it.next();
                if(event.getType() == EventType.TEMPERATURE) {
                    events.add(event);
                }
            }
            Collections.sort(events, (o1, o2) -> o1.getDate().compareTo(o2.getDate()));

            if(events.size() > 0) {
                try {
                    PrintWriter writer = new PrintWriter(database + "_" + instrumentName + "_Temperature.txt");
                    writer.print("Date\tTemperature\n");

                    // write the temperatures
                    for(Event event : events) {
                        writer.print(sdf.format(event.getDate()) + "\t");
                        writer.print(event.getExtra());
                        writer.println();
                    }
                    writer.close();
                } catch(FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        emf.close();
    }
}
