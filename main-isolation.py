import nltk
# Download and use the stopwords package
nltk.download('stopwords')

from experiments import experiment_1, experiment_2
from word import generate_word_file


if __name__ == '__main__':
    print("Generating word file....")
    generate_word_file()
    
    # experiment_1(rapids_off=True)
    # experiment_2(rapids_off=True)
    experiment_1()
    # experiment_2()
