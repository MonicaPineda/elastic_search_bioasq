import os
import subprocess
import json

def get_scores_phaseA(golden_file, test_json, path_home='/Evaluation-Measures/'):
    path_golden = path_home+'/golden_docs/'
    os.environ["PATH"] += os.pathsep + path_home + "jdk1.8.0_141/bin"
    os.environ["CLASSPATH"] = path_home+ "flat/BioASQEvaluation/dist/BioASQEvaluation.jar"
    
    out_file_name = path_home+'working_folder/test_'+golden_file
    json.dump(test_json, open(out_file_name, 'w'))
    process = subprocess.Popen(['java', 'evaluation.EvaluatorTask1b', '-phaseA', '-e', '5', 
                                path_golden+golden_file, 
                                out_file_name], 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    docs_scores = str(out).split(' ')[5:10]
    docs_scores = [float(i) for i in docs_scores]
    pass_scores = str(out).split(' ')[10:15]
    pass_scores = [float(i) for i in pass_scores]
    return docs_scores, pass_scores