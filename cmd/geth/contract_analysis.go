package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
)

type ContractMetaV1 struct {
	Owner                string
	AccountRoot          string
	StorageTrieKeySize   int
	StorageTrieValueSize int
	StorageTrieKeyNum    int
	ContractCodeHash     string
}

type ContractMetaV2 struct {
	Owner                  string
	AccountRoot            string
	StorageTrieKeySize     int
	StorageTrieValueSize   int
	StorageTrieNodeNum     int
	StorageTrieLeafNodeNum int
	ContractCodeHash       string
}

func readCSV(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip the first line (header)
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading header of CSV file %s: %v", filename, err)
	}
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV file %s: %v", filename, err)
	}

	return records, nil
}

func compareContractMetaData(baseFilename, compareFilename string) {
	// 读取基础版本的CSV文件
	baseRecords, err := readCSV(baseFilename)
	if err != nil {
		fmt.Println("Error reading base CSV:", err)
		return
	}

	// 读取对比版本的CSV文件
	compareRecords, err := readCSV(compareFilename)
	if err != nil {
		fmt.Println("Error reading compare CSV:", err)
		return
	}

	// 创建map来存储基础版本和对比版本的记录
	baseMap := make(map[string][]string)
	compareMap := make(map[string][]string)

	// 将基础版本的记录存储到map中
	for _, record := range baseRecords {
		owner := record[0]
		baseMap[owner] = record
	}

	// 将对比版本的记录存储到map中
	for _, record := range compareRecords {
		owner := record[0]
		compareMap[owner] = record
	}

	// 创建内存缓存来存储四种类型的记录
	var modifiedBaseRecords [][]string
	var modifiedCompareRecords [][]string
	var addedRecords [][]string
	var deletedRecords [][]string
	var unusedRecords [][]string

	// 遍历基础版本的记录
	for owner, baseRecord := range baseMap {
		// 获取对比版本中相同Owner的记录
		compareRecord, ok := compareMap[owner]
		if !ok {
			// 1) addedRecords, base 中存在, compare 内没有;
			addedRecords = append(addedRecords, baseRecord)
		} else {
			// 检查AccountRoot是否相同
			baseAccountRoot := baseRecord[1]
			compareAccountRoot := compareRecord[1]
			if baseAccountRoot != compareAccountRoot {
				/*
					如果AccountRoot不同，则将基础版本和对比版本的记录都标记为Modified类型
					2) modifiedBaseRecords: base 与 compare 内值不同, 保存的 base 内的值;
					modifiedCompareRecords: base 与 compare 内值不同, 保存的 compare 内的值;
				*/
				modifiedBaseRecords = append(modifiedBaseRecords, baseRecord)
				modifiedCompareRecords = append(modifiedCompareRecords, compareRecord)
			} else {
				// 如果AccountRoot相同，则将基础版本和对比版本的记录都标记为Unused类型
				/*
					3) unusedRecords: base 内与 compare 内值相同;
				*/
				unusedRecords = append(unusedRecords, compareRecord)
			}
		}
	}

	// 遍历对比版本的记录，标记为Added类型
	for owner, compareRecord := range compareMap {
		_, ok := baseMap[owner]
		if !ok {
			// 如果对比版本中没有相同Owner的记录，则将基础版本的记录标记为Deleted类型
			/*
				4) compare 内有, base 内没有, 表示已经删除;
			*/
			deletedRecords = append(deletedRecords, compareRecord)
		}
	}

	// 将内存缓存中的记录写入到CSV文件中
	writeContractMetaV2ToCSV("./data/modified_base.csv", modifiedBaseRecords, 1)
	writeContractMetaV2ToCSV("./data/modified_compare.csv", modifiedCompareRecords, 2)
	writeContractMetaV2ToCSV("./data/added.csv", addedRecords, 2)
	writeContractMetaV2ToCSV("./data/deleted.csv", deletedRecords, 1)
	writeContractMetaV2ToCSV("./data/unused.csv", unusedRecords, 2)
}

func writeContractMetaV2ToCSV(filename string, records [][]string, version int) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Error creating %s file: %v\n", filename, err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var headers []string
	// 写入表头
	if version == 1 {
		headers = []string{"Owner", "AccountRoot", "StorageTrieKeySize", "StorageTrieValueSize", "StorageTrieKeyNum", "ContractCodeHash"}
	} else if version == 2 {
		headers = []string{"Owner", "AccountRoot", "StorageTrieKeySize", "StorageTrieValueSize", "StorageTrieNodeNum", "StorageTrieLeafNodeNum", "ContractCodeHash"}
	}

	if err := writer.Write(headers); err != nil {
		fmt.Printf("Error writing headers to CSV: %v", err)
	}

	var totalKeySize, totalValueSize, totalTrieNodeNum, totalTrieLeafNodeNum uint64

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			fmt.Printf("Error writing record to %s file: %v\n", filename, err)
			return
		}
		// 将字符串转换为整数
		keySize, _ := strconv.Atoi(record[2])
		valueSize, _ := strconv.Atoi(record[3])

		var trieNodeNum, trieLeafNodeNum int
		if version == 1 {
			trieLeafNodeNum, _ = strconv.Atoi(record[4])
		} else if version == 2 {
			trieNodeNum, _ = strconv.Atoi(record[4])
			trieLeafNodeNum, _ = strconv.Atoi(record[5])
		}

		// 更新统计信息
		totalKeySize += uint64(keySize)
		totalValueSize += uint64(valueSize)
		totalTrieNodeNum += uint64(trieNodeNum)
		totalTrieLeafNodeNum += uint64(trieLeafNodeNum)
	}
	fmt.Printf("Write file: %v , number: %v , StorageTrieKeySize: %v , StorageTrieValueSize: %d , StorageTrieNodeNum: %v, StorageTrieLeafNodeNum: %d\n",
		filename, len(records), totalKeySize, totalValueSize, totalTrieNodeNum, totalTrieLeafNodeNum)
}

func topContractCodeHash(filePath string, topNum int) {
	// 读取基础版本的CSV文件
	records, err := readCSV(filePath)
	if err != nil {
		fmt.Println("Error reading base CSV:", err)
		return
	}

	// 统计ContractCodeHash的出现次数
	counts := make(map[string]int)
	for _, record := range records {
		contract := ContractMetaV1{
			Owner:                record[0],
			AccountRoot:          record[1],
			StorageTrieKeySize:   atoi(record[2]),
			StorageTrieValueSize: atoi(record[3]),
			StorageTrieKeyNum:    atoi(record[4]),
			ContractCodeHash:     record[5],
		}
		counts[contract.ContractCodeHash]++
	}

	// 对出现次数进行排序
	type kv struct {
		Key   string
		Value int
	}
	var sortedCounts []kv
	for k, v := range counts {
		sortedCounts = append(sortedCounts, kv{k, v})
	}
	sort.Slice(sortedCounts, func(i, j int) bool {
		return sortedCounts[i].Value > sortedCounts[j].Value
	})

	// 输出前10个ContractCodeHash
	fmt.Println("Top 10 ContractCodeHash:")
	for i := 0; i < topNum && i < len(sortedCounts); i++ {
		fmt.Printf("%d. %s: %d\n", i+1, sortedCounts[i].Key, sortedCounts[i].Value)
	}
}

func countContract(filePath string, version int) {
	// 读取基础版本的CSV文件
	records, err := readCSV(filePath)
	if err != nil {
		fmt.Println("Error reading base CSV:", err)
		return
	}

	var totalKeySize, totalValueSize, totalTrieNodeNum, totalTrieLeafNodeNum uint64
	for _, record := range records {
		// 将字符串转换为整数
		keySize, _ := strconv.Atoi(record[2])
		valueSize, _ := strconv.Atoi(record[3])

		/*
			v1 之前老的版本
			headers = []string{"Owner", "AccountRoot", "StorageTrieKeySize", "StorageTrieValueSize", "StorageTrieKeyNum", "ContractCodeHash"}
			v2 版本, 多加了一个 StorageTrieLeafNodeNum
			headers = []string{"Owner", "AccountRoot", "StorageTrieKeySize", "StorageTrieValueSize", "StorageTrieNodeNum", "StorageTrieLeafNodeNum", "ContractCodeHash"}
			Owner,AccountRoot,StorageTrieKeySize,StorageTrieValueSize,StorageTrieNodeNum,StorageTrieLeafNodeNum,ContractCodeHash
		*/
		var trieNodeNum, trieLeafNodeNum int
		if version == 1 {
			trieLeafNodeNum, _ = strconv.Atoi(record[4])
		} else if version == 2 {
			trieNodeNum, _ = strconv.Atoi(record[4])
			trieLeafNodeNum, _ = strconv.Atoi(record[5])
		}

		// 更新统计信息
		totalKeySize += uint64(keySize)
		totalValueSize += uint64(valueSize)
		totalTrieNodeNum += uint64(trieNodeNum)
		totalTrieLeafNodeNum += uint64(trieLeafNodeNum)
	}

	totalKeySizeGB := float64(totalKeySize) / (1024 * 1024 * 1024)
	avgKeySize := float64(totalKeySize) / float64(len(records))

	totalValueSizeGB := float64(totalValueSize) / (1024 * 1024 * 1024)
	avgValueSize := float64(totalValueSize) / float64(len(records))
	fmt.Printf("Total StorageTrieKeySize: %.2f, avgKeySize: %.2f\n", totalKeySizeGB, avgKeySize)
	fmt.Printf("Total StorageTrieValueSize: %.2f, avgValueSize: %.2f\n", totalValueSizeGB, avgValueSize)
	fmt.Printf("Total StorageTrieKeyNum: %d\n", len(records))
	fmt.Printf("Count number: %v , StorageTrieKeySize: %v , StorageTrieValueSize: %v , StorageTrieNodeNum: %v, StorageTrieLeafNodeNum: %d\n",
		len(records), totalKeySize, totalValueSize, totalTrieNodeNum, totalTrieLeafNodeNum)
}

func main() {
	// 创建一个新的命令集合
	// 注册子命令及其对应的标志
	/*
		1) process
		2) top
		3) count
		./main count --path /Users/lijingjun/contract_meta_20240412.csv
		./main count --path /Users/lijingjun/contract_meta_20240412.csv --version 2 1
		1
	*/
	processCmd := flag.NewFlagSet("process", flag.ExitOnError)
	basePath := processCmd.String("base", "", "Path to base CSV file")
	comparePath := processCmd.String("compare", "", "Path to compare CSV file")

	topCmd := flag.NewFlagSet("top", flag.ExitOnError)
	topPath := topCmd.String("path", "", "Path to CSV file")
	topNum := topCmd.Int("num", 10, "top num")

	countCmd := flag.NewFlagSet("count", flag.ExitOnError)
	countPath := countCmd.String("path", "", "Path to CSV file")
	countVersion := countCmd.Int("version", 2, "count version")

	// 解析命令行参数
	if len(os.Args) < 2 {
		fmt.Println("Usage: myapp <command> [<args>]")
		fmt.Println("Commands:")
		fmt.Println("  process - Compare two CSV files")
		fmt.Println("  top - Display top entries of a CSV file")
		return
	}

	// 根据第一个参数选择执行哪个子命令
	switch os.Args[1] {
	case "process":
		processCmd.Parse(os.Args[2:])
		compareContractMetaData(*basePath, *comparePath)
	case "top":
		topCmd.Parse(os.Args[2:])
		topContractCodeHash(*topPath, *topNum)
	case "count":
		countCmd.Parse(os.Args[2:])
		countContract(*countPath, *countVersion)
	default:
		fmt.Println("Unknown command:", os.Args[1])
	}
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		n = n*10 + int(r-'0')
	}
	return n
}
