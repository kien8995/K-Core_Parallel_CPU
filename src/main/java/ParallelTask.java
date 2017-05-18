import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

import edu.rit.pj2.Loop;
import edu.rit.pj2.Task;

public class ParallelTask extends Task {

	private Map<String, Float> kCore;
	private PriorityQueue<Vertex> vertexQueue;
	private Vector<String> listVertex;
	private Map<String, Integer> degrees;

	public ParallelTask(Map<String, Float> kCore, PriorityQueue<Vertex> vertexQueue, Vector<String> listVertex,
			Map<String, Integer> degrees) {
		this.kCore = kCore;
		this.vertexQueue = vertexQueue;
		this.listVertex = listVertex;
		this.degrees = degrees;
	}

	@Override
	public void main(String[] arg0) throws Exception {

		int length = listVertex.size();

		parallelFor(0, length - 1).exec(new Loop() {

			@Override
			public void run(int index) throws Exception {
				String vertex = listVertex.get(index);

				if (!kCore.containsKey(vertex)) {
					degrees.put(vertex, degrees.get(vertex) - 1);
					vertexQueue.add(new Vertex(vertex, degrees.get(vertex)));
				}

			}

		});

	}

}
