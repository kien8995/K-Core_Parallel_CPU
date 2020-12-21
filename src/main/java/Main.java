import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

public class Main {
	// path to input/output file
	private static final String INPUT = "SequenceAssociation.txt";
	private static final String OUTPUT = "output.txt";

	// list to store edges
	private List<Edge> edgeList;
	// map to store k-core
	private Map<String, Float> kCore;
	// map to store adjacency list
	private Map<String, Vector<String>> adjList;
	// map to store degree
	private Map<String, Integer> degrees;
	// vertex queue
	private PriorityQueue<Vertex> vertexQueue;

	// main function
	public static void main(String[] args) throws Exception {
		int mb = 1024 * 1024;
		// Getting the runtime reference from system
		Runtime runtime = Runtime.getRuntime();
		System.out.println("##### Heap utilization statistics [MB] #####");

		Main main = new Main();
		main.init();
		main.readFile();
		main.loadData();
		long start = System.currentTimeMillis();
		main.compute();
		long end = System.currentTimeMillis();
		System.out.println(end - start);
		main.writeFile();

		// Print used memory
		System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
	}

	// initialize
	public void init() {
		edgeList = new ArrayList<Edge>();
		kCore = new HashMap<String, Float>();
		adjList = new HashMap<String, Vector<String>>();
		degrees = new ConcurrentHashMap<String, Integer>();
		vertexQueue = new PriorityQueue<Vertex>();
	}

	// read input.txt and convert edge list to adjacency list
	public void readFile() {

		Path path = Paths.get(INPUT);

		try (Stream<String> lines = Files.lines(path)) {
			Spliterator<String> lineSpliterator = lines.spliterator();
			Spliterator<Edge> edgeSpliterator = new EdgeSpliterator(lineSpliterator);

			Stream<Edge> edgeStream = StreamSupport.stream(edgeSpliterator, false);
			edgeStream.forEach(edge -> edgeList.add(edge));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// load data
	public void loadData() {
		for (Edge edge : edgeList) {
			pushMap(adjList, edge.getStartNode(), edge.getEndNode());
			pushMap(adjList, edge.getEndNode(), edge.getStartNode());
		}

		for (Map.Entry<String, Vector<String>> entry : adjList.entrySet()) {
			degrees.put(entry.getKey(), entry.getValue().size());
		}

		for (Map.Entry<String, Integer> entry : degrees.entrySet()) {
			vertexQueue.add(new Vertex(entry.getKey(), entry.getValue()));
		}
	}

	// write result to output.txt
	public void writeFile() throws Exception {

		Path path = Paths.get(OUTPUT);
		List<String> lines = new ArrayList<>();
		// sort map by value
		Map<String, Float> sortedMap = MapComparator.sortByValue(kCore);

		for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
			lines.add(String.format("%s\t%.0f", entry.getKey(), entry.getValue()));
		}

		Files.write(path, lines);

		// writeXLSFile(sortedMap);
	}

	// push value to map
	public void pushMap(Map<String, Vector<String>> adjList, String start, String end) {
		if (!adjList.containsKey(start)) {
			adjList.put(start, new Vector<>());
		}
		adjList.get(start).add(end);
	}

	// compute
	public void compute() throws Exception {
		int k = 0;
		// BFS traverse
		while (vertexQueue.size() != 0) {
			Vertex current = vertexQueue.poll();
			if (degrees.get(current.getVertex()) < current.getDegree()) {
				continue;
			}

			k = Math.max(k, degrees.get(current.getVertex()));

			kCore.put(current.getVertex(), Float.valueOf(k));

			new ParallelTask(kCore, vertexQueue, adjList.get(current.getVertex()), degrees).main(new String[] {});

		}
		System.out.println("K-Core: " + k);
	}

	public void writeXLSFile(Map<String, Float> result) throws IOException {

		// name of excel file
		String excelFileName = "result.xls";

		// name of sheet
		String sheetName = "Sheet1";

		HSSFWorkbook wb = new HSSFWorkbook();
		HSSFSheet sheet = wb.createSheet(sheetName);
		HSSFRow row;
		HSSFCell cell;

		// header
		row = sheet.createRow(0);
		cell = row.createCell(0);
		cell.setCellValue("Node");
		cell = row.createCell(1);
		cell.setCellValue("Rank");

		int index = 1;
		for (Map.Entry<String, Float> entry : result.entrySet()) {
			row = sheet.createRow(index++);

			cell = row.createCell(0);
			cell.setCellValue(String.format("%s", entry.getKey()));

			cell = row.createCell(1);
			cell.setCellValue(String.format("%.0f", entry.getValue()));
		}

		FileOutputStream fileOut = new FileOutputStream(excelFileName);

		// write this workbook to an Outputstream.
		wb.write(fileOut);
		fileOut.flush();
		fileOut.close();
	}
}
