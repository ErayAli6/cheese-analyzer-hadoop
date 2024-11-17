package fmi.cheeseanalyzer_ErayAli_2101321032;

import java.awt.Color;
import java.awt.Font;
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
		init();
	}

	private void init() {
		JPanel panel = new JPanel();
		panel.setLayout(null);
		setSize(800, 900);
		setTitle("Cheese Analyzer");
		getContentPane().setBackground(Color.WHITE);

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
		analyzeButton.setFont(new Font("Arial", Font.BOLD, 14));
		analyzeButton.setBackground(new Color(59, 89, 182));
		analyzeButton.setForeground(Color.WHITE);
		analyzeButton.setFocusPainted(false);

		resultOutput = new JTextArea();
		resultOutput.setEditable(false);
		resultOutput.setFont(new Font("Monospaced", Font.PLAIN, 14));
		JScrollPane scrollPane = new JScrollPane(resultOutput);

		JLabel parametersLabel = new JLabel("Analysis Parameters:");
		JLabel provLabel = new JLabel("Province:");
		JLabel catLabel = new JLabel("Category:");
		JLabel milkLabel = new JLabel("Milk Type:");
		JLabel calcLabel = new JLabel("Calculation:");
		JLabel resultsLabel = new JLabel("Analysis Results:");

		int labelWidth = 100;
		int comboWidth = 200;
		int height = 25;
		int startX = 50;
		int gapY = 40;

		parametersLabel.setBounds(startX, 20, 200, height);

		provLabel.setBounds(startX, 60, labelWidth, height);
		provCodeCombo.setBounds(startX + labelWidth + 10, 60, comboWidth, height);

		catLabel.setBounds(startX, 60 + gapY, labelWidth, height);
		categoryCombo.setBounds(startX + labelWidth + 10, 60 + gapY, comboWidth, height);

		milkLabel.setBounds(startX, 60 + 2 * gapY, labelWidth, height);
		milkTypeCombo.setBounds(startX + labelWidth + 10, 60 + 2 * gapY, comboWidth, height);

		calcLabel.setBounds(startX, 60 + 3 * gapY, labelWidth, height);
		calculationType.setBounds(startX + labelWidth + 10, 60 + 3 * gapY, comboWidth, height);

		analyzeButton.setBounds(startX + labelWidth + 10, 60 + 4 * gapY + 10, comboWidth, height + 10);

		resultsLabel.setBounds(startX, 60 + 5 * gapY + 40, 200, height);
		
		scrollPane.setBounds(startX, 60 + 5 * gapY + 70, 700, 550);

		panel.add(parametersLabel);
		panel.add(provLabel);
		panel.add(provCodeCombo);
		panel.add(catLabel);
		panel.add(categoryCombo);
		panel.add(milkLabel);
		panel.add(milkTypeCombo);
		panel.add(calcLabel);
		panel.add(calculationType);
		panel.add(analyzeButton);
		panel.add(resultsLabel);
		panel.add(scrollPane);

		add(panel);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLocationRelativeTo(null);
		setVisible(true);

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