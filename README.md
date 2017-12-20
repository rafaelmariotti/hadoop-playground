# hadoop-playground
Playground from Hadoop, with some helpful Python 3.5 scripts

## Configure

        pip install mrjob

## HowTo

        python mapReduce/movie-rating-counter.py dataset/movieLens/u.data
        python mapReduce/friends-by-age.py dataset/fakeFriends/fakefriends.csv
        python mapReduce/min-temperatures.py dataset/1800temperatures/1800.csv
        python mapReduce/max-temperatures.py dataset/1800temperatures/1800.csv
        python word-frequency.py dataset/books/book_example.txt
        python word-frequency-sorted.py dataset/books/book_example.txt
        python word-frequency-combiner.py dataset/books/book_example.txt
        python spend-by-customer.py dataset/customerOrders/customer-orders.csv
        python spend-by-customer-sorted.py dataset/customerOrders/customer-orders.csv
        python word-frequency-combiner.py dataset/books/book_example.txt
        python word-frequency-combiner-sorted.py dataset/books/book_example.txt
        python movie-most-times-rated.py dataset/movieLens/u.data
        python movie-most-times-rated-nicer.py dataset/movieLens/u.data --items dataset/movieLens/u.item
        python marvel-most-popular-superhero.py dataset/marvelHeroes/marvel-graph.txt --names dataset/marvelHeroes/marvel-names.txt
        python marvel-breadth-first-search-distance-between-superheroes-prepare.py 2548 ; python marvel-breadth-first-search-distance-between-superheroes.py --target 100 dataset/marvelHeroes/breadth-first-search-2548.txt > iteration-0.output ; python marvel-breadth-first-search-distance-between-superheroes.py --target 100 iteration-0.output > iteration-1.output
        python movie-similarities.py dataset/movieLens/u.data --movies dataset/movieLens/u.item

## Running using EMR from AWS
        python movie-similarities.py --movies dataset/movieLens/u.item dataset/movieLens/u.data -r emr
        python movie-similarities.py --movies dataset/movieLens/u.item dataset/movieLens/u.data -r emr --num-core-instances 4 #https://pythonhosted.org/mrjob/guides/emr-opts.html#option-num_core_instances

## Retrieve AWS EMR errors ([click here to learn more about it](http://mrjob.readthedocs.io/en/latest/index.html))
        mrjob diagnose cluster_ID #for instance, j-1SHKB7PLLVKYP 