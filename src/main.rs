use csv::{ReaderBuilder, Writer};
use std::{cmp, collections::BTreeSet, env, error, fs, io, mem, sync, sync::mpsc::channel, time::Instant};
use human_sort::compare;
use threadpool;

fn main() {
    const DEFAULT_TREE_DEPTH : usize = 13;

    let mut max_primes_count = 0;
    let mut tree_depth = DEFAULT_TREE_DEPTH;
    let mut write_file = false;
    let mut load_file = false;

    let mut print_help = true;

    let mut command = "".to_string();
    let mut current_arg = "".to_string();
    for arg in env::args() {
        if command == "" {
            command = arg;
            continue;
        }
        print_help = false;
        if current_arg == "" {
            if arg == "-w" {
                write_file = true;
                continue;
            }
            if arg == "-l" {
                load_file = true;
                continue;
            }
            if arg == "-help" || arg == "/?" {
                print_help = true;
            }
            current_arg = arg;
            continue;
        }
        match current_arg.as_str() {
            "-n" => max_primes_count = arg.parse().unwrap(),
            "-c" => tree_depth = arg.parse().unwrap(),
            _ => println!("unknown paramter {}", current_arg),
        }
        current_arg = "".to_string();
    }
    if print_help || max_primes_count == 0 {
        println!("Supported arguments:");
        println!("-n number     how many primes to find");
        println!("-t number     max tree depth for calculations. Don't go too high or the system might become unstable. default value: {}", DEFAULT_TREE_DEPTH);
        println!("-w            write output to file, the file will be named 'number.txt' - where number is the amount of primes calculated");
        println!("-l            attempt to load previously calculated primes to get a head start");
        println!("sample use:");
        println!("{} -n 1000000000 -t 13", command);
        return;
    }
    if current_arg.len() != 0 {
        println!("Parameter without value: {}", current_arg);
    }
    
    let mut primes = get_primes_input(load_file);
    primes.reserve(max_primes_count/20*21);//reserve enough space for all primes and then some (alsorithm will most likely overshoot by a little)

    let max_chunk_size = calculate_tree_size(tree_depth);
    let result = calculate_primes(primes, max_primes_count, max_chunk_size);

    println!("prime number {} one is {}", max_primes_count, result.get(max_primes_count - 1).unwrap());
    if write_file {
        write_primes(&result);
    }
    let mut current_prime_position = max_primes_count;
    println!("Enter a prime index to show the prime number that matches said index. use '+' or '-' to increase the previously displayed prime index. To stop the application, type 'exit'.");
    let stdin = io::stdin();
    loop {
        let mut input = String::new();
        let rdln_result = stdin.read_line(& mut input);
        input = input.replace("\r", "");
        input = input.replace("\n", "");
        if rdln_result.is_err() || input == "exit" {
            println!("bye bye!");
            break;
        }
        if input == "+" {
            if current_prime_position == usize::MAX {
                println!("Can't go higher than {} - I just can't. You won't have this prime any way!", current_prime_position);
                continue;
            }
            current_prime_position += 1;
        } else if input == "-" {
            if current_prime_position == 0 {
                println!("The first prime is at position 1 - there is no position 0. Why would you even want to go lower?");
                continue;
            }
            current_prime_position -= 1;
        } else {
            let numeric_input = input.parse();
            if numeric_input.is_err() {
                println!("Sorry - I did not understaind {}, please try again.", input);
                println!("Enter a prime index to show the prime number that matches said index. use '+' or '-' to increase the previously displayed prime index. To stop the application, type 'exit'.");
                continue;
            }
            current_prime_position = numeric_input.unwrap();
        }
        if current_prime_position == 0 {
            println!("The first prime is at position 1 - there is no position 0.");
            continue;
        }
        let value = result.get(current_prime_position - 1);//0-based
        if value.is_none() {
            println!("Prime #{} could not be found.", current_prime_position);
            continue;
        }
        println!("Prime #{} is {}.", current_prime_position, value.unwrap());
    }
}

fn calculate_tree_size(depth: usize) -> usize {
    if depth == 1 {
        return 1
    }
    let base: usize = 2;
    let leaves = base.pow((depth - 1) as u32);
    return calculate_tree_size(depth - 1) + leaves;
}

fn write_primes(primes: &Vec<i64>) {
    println!("writing to file...");
    let mut wtr = Writer::from_path(primes.len().to_string() + ".csv").expect("I wants the file");
    for ele in primes.iter() {
        wtr.write_record(&[ele.to_string()]).expect("come on, just write");
    }
    wtr.flush().expect("there there, flush it down");
    println!("wrote {} primes", primes.len());
}

fn get_primes_input(load_primes: bool) -> Vec<i64> {
    if !load_primes {
        return get_default_primes_input();
    }
    let loaded_primes = get_primes_input_from_file();
    let primes = match loaded_primes {
        Ok(x) => {
            if x.len() == 0 {
                return get_default_primes_input()
            }
            x
        },
        Err(_) => {
            get_default_primes_input()
        }
    };
    
    primes
}

fn get_default_primes_input() -> Vec<i64> {
    let primes = vec!{2, 3};
    primes
}

fn get_primes_input_from_file() -> Result<Vec<i64>, Box<dyn error::Error>>{
    let current_dir = env::current_dir()?;
    let mut best_file_name = "".to_string();
    for item in fs::read_dir(current_dir)? {
        let file = item?;
        let path = file.path();
        let extension = path.extension();
        if extension.is_none() || extension.unwrap() != "csv" {
            continue;
        }
        let filename = file.file_name();
        let filename_string = filename.to_string_lossy().to_string();
        if compare(filename_string.as_str(), best_file_name.as_str()) == cmp::Ordering::Greater {
            best_file_name = filename_string;
        }
    }

    let mut primes : Vec<i64> = Vec::new();
    let mut rdr = ReaderBuilder::new()
        .from_path(best_file_name)?;

    println!("reading file - this may take a while...");

    let headers = rdr.headers()?;
    let initial_value = headers.get(0);
    if initial_value.is_none() {
        return Ok(primes);
    }

    primes.push(initial_value.unwrap().parse()?);
    println!("parsed first file line");
    
    primes.extend(rdr.records().into_iter().map(|x| x.unwrap().as_slice().parse::<i64>().unwrap()));
    
    println!("loaded {} primes from file.", primes.len());
    Ok(primes)
}

fn calculate_primes(primes_input: Vec<i64>, max_primes_count: usize, max_chunk_size: usize) -> Vec<i64> {
    let actual_max_chunk_size = max_chunk_size * 2;//since we only add uneven elements, the actual chunk size for this function is doubled
    let mut primes_len = primes_input.len();
    let primes = sync::Arc::new(std::sync::RwLock::new(primes_input));
    let mut last_checked = primes.read().unwrap().last().expect("should exist").clone();

    let pool = threadpool::Builder::new().build();
    
    let start = Instant::now();
    while primes_len < max_primes_count {
        let (mut max_to_calculate, overflow) = last_checked.overflowing_mul(last_checked);
        if overflow {
            max_to_calculate = i64::MAX;
        }
        
        let (tx, rx) = channel();

        //based on how much we had to calculate in the past, guess how much we'll have to calculate to get to the requested amount of primes
        let guestimate_threads_needed = (max_primes_count - primes_len) as i64 * last_checked as i64 / primes_len as i64 / actual_max_chunk_size as i64 + 1;

        println!("guestimate: {}, last checked: {}, primes len: {}, chunk size {}", guestimate_threads_needed, last_checked, primes_len, max_chunk_size);

        for _ in 0..guestimate_threads_needed {
            let primes = primes.clone();
            let to = cmp::min(max_to_calculate, actual_max_chunk_size as i64 + last_checked);
            let tx = tx.clone();
            pool.execute(move|| {
                let new_primes = sieve_primes(&primes.read().unwrap(), last_checked + 2, to);
                tx.send(new_primes).unwrap();
            });

            last_checked = to;
            if to == max_to_calculate {
                break;
            }
        }
        pool.join();
        drop(tx);
        let mut results : Vec<Vec<i64>> = rx.iter().collect();
        results.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
        for new_primes in results {
            primes.write().unwrap().extend(new_primes);
        }
        primes_len = primes.read().unwrap().len();
    }
    
    let duration = start.elapsed();
    println!("Time elapsed to generate the first {} prime numbers: {:?}", primes_len, duration);

    let mut tmp = primes.write().unwrap();
    mem::take(&mut *tmp)
}

fn sieve_primes(previous_primes: &Vec<i64>, from: i64, to: i64) -> Vec<i64> {
    let mut primes = BTreeSet::new();
    //fill up with all possibilities
    let sqrt_to = (to as f64).sqrt() as i64;
    for possible_prime in (from..=to).step_by(2) {
        primes.insert(possible_prime);
    }
    for prev_prime in previous_primes.iter().skip(1) {
        if *prev_prime > sqrt_to {
            break;
        }
        let mut to_remove = (from / prev_prime) * prev_prime;
        if to_remove % 2 == 0 {
            to_remove += prev_prime;
        }
        while to_remove <= to {
            primes.remove(&to_remove);
            to_remove += prev_prime + prev_prime;
        }
    }
    primes.into_iter().collect()
}