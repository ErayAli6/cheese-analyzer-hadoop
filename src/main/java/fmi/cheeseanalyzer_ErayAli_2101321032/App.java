package fmi.cheeseanalyzer_ErayAli_2101321032;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import javax.swing.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class App extends JFrame {
	private JTextArea resultOutput;
	private JComboBox<String> provCodeCombo;
	private JComboBox<String> categoryCombo;
	private JComboBox<String> milkTypeCombo;
	private JComboBox<String> calculationType;

	public static void main(String[] args) {
		new App();
	}

	public App() {
		initializeFrame();
	}

	private void initializeFrame() {
		JPanel panel = createPanel();
		add(panel);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setSize(800, 900);
		setVisible(true);
	}

	private JPanel createPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(null);

		initializeComponents(panel);

		return panel;
	}

	private void initializeComponents(JPanel panel) {
		String[] manufacturerProvCodeOptions = { "All", "AB", "BC", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK" };
		String[] categoryTypeOptions = { "All", "Firm Cheese", "Fresh Cheese", "Hard Cheese", "Semi-soft Cheese",
				"Soft Cheese", "Veined Cheese" };
		String[] milkTypeOptions = { "All", "Cow", "Goat", "Ewe", "Cow and Goat", "Cow and Ewe", "Goat and Ewe",
				"Cow, Goat and Ewe", "Buffalo Cow" };
		String[] calcTypes = { "Average Moisture", "Organic Percentage" };

		provCodeCombo = new JComboBox<>(manufacturerProvCodeOptions);
		categoryCombo = new JComboBox<>(categoryTypeOptions);
		milkTypeCombo = new JComboBox<>(milkTypeOptions);
		calculationType = new JComboBox<>(calcTypes);

		JButton analyzeButton = new JButton("Analyze");
		resultOutput = new JTextArea();
		resultOutput.setEditable(false);
		JScrollPane scrollPane = new JScrollPane(resultOutput);

		JLabel provLabel = new JLabel("Province:");
		JLabel catLabel = new JLabel("Category:");
		JLabel milkLabel = new JLabel("Milk Type:");
		JLabel calcLabel = new JLabel("Calculation:");

		provLabel.setBounds(50, 50, 100, 25);
		provCodeCombo.setBounds(150, 50, 200, 25);

		catLabel.setBounds(50, 90, 100, 25);
		categoryCombo.setBounds(150, 90, 200, 25);

		milkLabel.setBounds(50, 130, 100, 25);
		milkTypeCombo.setBounds(150, 130, 200, 25);

		calcLabel.setBounds(50, 170, 100, 25);
		calculationType.setBounds(150, 170, 200, 25);

		analyzeButton.setBounds(150, 220, 200, 30);
		scrollPane.setBounds(50, 270, 700, 550);

		panel.add(provLabel);
		panel.add(provCodeCombo);
		panel.add(catLabel);
		panel.add(categoryCombo);
		panel.add(milkLabel);
		panel.add(milkTypeCombo);
		panel.add(calcLabel);
		panel.add(calculationType);
		panel.add(analyzeButton);
		panel.add(scrollPane);

		analyzeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				startHadoopAnalysis();
			}
		});
	}

	private void startHadoopAnalysis() {
		Configuration conf = new Configuration();
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/18.csv");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/result");
		JobConf job = new JobConf(conf, App.class);

		job.set("provCode", provCodeCombo.getSelectedItem().toString());
		job.set("category", categoryCombo.getSelectedItem().toString());
		job.set("milkType", milkTypeCombo.getSelectedItem().toString());
		job.set("calcType", calculationType.getSelectedItem().toString());

		job.setMapperClass(CheeseMapper.class);
		job.setReducerClass(CheeseReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		FileInputFormat.setInputPaths(job, inputPath);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), job);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			RunningJob task = JobClient.runJob(job);
			if (task.isSuccessful()) {
				readResultFile(fs);
			} else {
				resultOutput.setText("Error processing data");
			}
		} catch (IOException e) {
			resultOutput.setText("Error: " + e.getMessage());
		}
	}

	private void readResultFile(FileSystem fs) throws IOException {
		Path resultFile = new Path("hdfs://127.0.0.1:9000/result/part-00000");
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(resultFile)));
		resultOutput.setText("");
		String line;
		while ((line = br.readLine()) != null) {
			resultOutput.append(line + "\n");
		}
		br.close();
	}
}