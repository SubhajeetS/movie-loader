package reader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import loader.MovieQueue;
import loader.domain.Title;

public class MovieReader implements Runnable{
	private MovieQueue queue;
	private CountDownLatch latch;
	private String type;
	
	public MovieReader(MovieQueue queue, CountDownLatch latch, String type) {
		this.queue = queue;
		this.latch = latch;
		this.type = type;
	}
	
	abstract class RowProcessor extends ObjectRowProcessor {
		
		public abstract String getFileName();
		public abstract void processRow(Object[] row);
		
		protected Map<String, Integer> columnsIndex = new HashMap<>();
		
		@Override
		public void processEnded(ParsingContext context) {
			queue.done();
			System.out.println("Read Done");
			latch.countDown();
		}
		
		protected boolean isHeader(ParsingContext context) {
			return context.currentLine() == 1;
		}
		
		protected void setHeaders(Object[] row) {
			for(int index = 0; index < row.length; index++) {
				String key = (String)row[index];
				columnsIndex.put(key, index);
			}
		}
		
		protected Object getValue(Object[] row, String property) {
			Integer index = columnsIndex.get(property);
			return row[index];
		}
		
		@Override
	    public void rowProcessed(Object[] row, ParsingContext context) {
			if(isHeader(context)) {
				setHeaders(row);
			} else {
				processRow(row);
			}
	    }
	}

	class RatingsRowProcessor extends RowProcessor {
		private final String file = "data/title.ratings.tsv";

		private Title getRating(Object[] row) {
			Title t = new Title();
			t.setTconst((String)(getValue(row, "tconst")));
			t.setAverageRating((String)(getValue(row, "averageRating")));
			t.setNumVotes(Long.parseLong((String)(getValue(row, "numVotes"))));
			return t;
		}
		@Override
		public String getFileName() {
			return file;
		}
		
		@Override
		public void processRow(Object[] row) {
			Title t = getRating(row);
	        queue.put(t);
		}
		
	}
	
	class TitlesRowProcessor extends RowProcessor {
		public final String file = "data/title.basics.tsv";

		private Title getTitle(Object[] row) {
			Title t = new Title();
			t.setTconst((String)(getValue(row, "tconst")));
			t.setPrimaryTitle(((String)(getValue(row, "primaryTitle"))).toLowerCase());
			t.setOriginalTitle(((String)getValue(row, "originalTitle")).toLowerCase());
			t.setStartYear((String) getValue(row, "startYear"));
			t.setGenres((String)getValue(row, "genres"));
			return t;
		}
		
		@Override
		public String getFileName() {
			return file;
		}
		
		@Override
		public void processRow(Object[] row) {
			Title t = getTitle(row);
	        queue.put(t);
		}
	}

	@Override
	public void run() {
		
		TsvParserSettings settings = new TsvParserSettings();

		RowProcessor pr = null;
		
		if(type == "RATINGS") {
			pr = new RatingsRowProcessor();			
		} else {
			pr = new TitlesRowProcessor();
		}
		
		settings.setProcessor(pr);

		try {
			
			TsvParser parser = new TsvParser(settings);
			parser.parse(new FileReader(pr.getFileName()));
			
		} catch (FileNotFoundException e) {
			//log to error.log
			e.printStackTrace();
		}

	}

}
