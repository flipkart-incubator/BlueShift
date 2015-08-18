/*
 *
 *  Copyright 2015 Flipkart Internet Pvt. Ltd.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.flipkart.fdp.optimizer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;
import com.google.common.collect.Lists;

public class Driver {

	private static final int numberOfProcessors = 11;

	public static void main(String args[]) throws IOException {
		/*
		 * String[] inputs = new
		 * String[]{"topic,1,40","topic,1,50","topic,1,40","topic,1,5",
		 * "topic,1,5"
		 * ,"topic,1,90","topic,1,30","topic,1,20","topic,1,10","topic,1,30",
		 * "topic,1,5","topic,1,5","topic,1,5"};
		 */
		/*
		 * String[] inputs = new
		 * String[]{"topic,1,96009675","topic,1,13786154","topic,1,93798139"
		 * ,"topic,1,5710549",
		 * "topic,1,90441353","topic,1,63896560","topic,1,61404179"
		 * ,"topic,1,49694777","topic,1,49492367","topic,1,45129627"};
		 */
		/*
		 * String[] inputs = new
		 * String[]{"topic,1,1","topic,1,40","topic,1,1598"
		 * ,"topic,1,49","topic,1,6859","topic,1,5520",
		 * "topic,1,7272","topic,1,7684"
		 * ,"topic,1,32","topic,1,82","topic,1,165",
		 * "topic,1,840","topic,1,868","topic,1,2146",
		 * "topic,1,3095","topic,1,680"
		 * ,"topic,1,8003","topic,1,2931","topic,1,808","topic,1,5"};
		 */
		String[] inputs = new String[] { "topic,1,404263", "topic,1,223368",
				"topic,1,533135", "topic,1,13481", "topic,1,12937",
				"topic,1,6048", "topic,1,138856", "topic,1,32747",
				"topic,1,52906", "topic,1,92116", "topic,1,6859",
				"topic,1,24251", "topic,1,67380", "topic,1,53212",
				"topic,1,7563", "topic,1,84969", "topic,1,544511",
				"topic,1,95177", "topic,1,88801", "topic,1,8658",
				"topic,1,763203", "topic,1,84739", "topic,1,36516",
				"topic,1,651060", "topic,1,68711", "topic,1,40329",
				"topic,1,1682", "topic,1,2517", "topic,1,943687",
				"topic,1,3113", "topic,1,95263", "topic,1,96310",
				"topic,1,74662", "topic,1,77486", "topic,1,492921",
				"topic,1,208194", "topic,1,3659", "topic,1,35801",
				"topic,1,8880", "topic,1,90325", "topic,1,88116",
				"topic,1,72830", "topic,1,34350", "topic,1,8705",
				"topic,1,83992", "topic,1,474666", "topic,1,67828",
				"topic,1,68601", "topic,1,32851", "topic,1,5636",
				"topic,1,413800", "topic,1,158892", "topic,1,5283",
				"topic,1,86739", "topic,1,8724", "topic,1,8833",
				"topic,1,984921", "topic,1,5746", "topic,1,797042",
				"topic,1,9979", "topic,1,6914", "topic,1,62675",
				"topic,1,93362", "topic,1,68751", "topic,1,81267",
				"topic,1,260109", "topic,1,438227", "topic,1,4444",
				"topic,1,8716", "topic,1,109412", "topic,1,126140",
				"topic,1,5992", "topic,1,56378", "topic,1,701444",
				"topic,1,483465", "topic,1,8272", "topic,1,30490",
				"topic,1,610738", "topic,1,9041", "topic,1,44458",
				"topic,1,3187", "topic,1,501046", "topic,1,66325",
				"topic,1,3732", "topic,1,99276", "topic,1,13768",
				"topic,1,32081", "topic,1,575470", "topic,1,470137",
				"topic,1,893575", "topic,1,2604", "topic,1,6731",
				"topic,1,8199", "topic,1,80580", "topic,1,590190",
				"topic,1,6739", "topic,1,32368", "topic,1,2742",
				"topic,1,2368", "topic,1,61701", "topic,1,85847",
				"topic,1,8826", "topic,1,34126", "topic,1,2375",
				"topic,1,23638", "topic,1,513530", "topic,1,39191",
				"topic,1,5604", "topic,1,153867", "topic,1,6676",
				"topic,1,7424", "topic,1,344893", "topic,1,38688",
				"topic,1,13674", "topic,1,101442", "topic,1,4703",
				"topic,1,928446", "topic,1,17737", "topic,1,26127",
				"topic,1,5233", "topic,1,198293", "topic,1,1441",
				"topic,1,489826", "topic,1,28512", "topic,1,44931",
				"topic,1,879783", "topic,1,921", "topic,1,84137",
				"topic,1,5308", "topic,1,1457", "topic,1,14634",
				"topic,1,503958", "topic,1,118077", "topic,1,87565",
				"topic,1,3773", "topic,1,44626", "topic,1,333757",
				"topic,1,443256", "topic,1,351285", "topic,1,35568",
				"topic,1,419724", "topic,1,956", "topic,1,30598",
				"topic,1,5996", "topic,1,1816", "topic,1,71504",
				"topic,1,715870", "topic,1,39723", "topic,1,825524",
				"topic,1,563212", "topic,1,850340", "topic,1,610467",
				"topic,1,36796", "topic,1,969811", "topic,1,4363",
				"topic,1,438141", "topic,1,33316", "topic,1,8475",
				"topic,1,93334", "topic,1,427149", "topic,1,393851",
				"topic,1,9564", "topic,1,87886", "topic,1,17536",
				"topic,1,3679", "topic,1,1285", "topic,1,70", "topic,1,46856",
				"topic,1,43453", "topic,1,14819", "topic,1,88796",
				"topic,1,5113", "topic,1,331593", "topic,1,265414",
				"topic,1,567893", "topic,1,5008", "topic,1,395070",
				"topic,1,2004", "topic,1,552758", "topic,1,2554",
				"topic,1,7439", "topic,1,7531", "topic,1,9410",
				"topic,1,43668", "topic,1,68881", "topic,1,986896",
				"topic,1,726117", "topic,1,837480", "topic,1,353230",
				"topic,1,19520", "topic,1,152336", "topic,1,980186",
				"topic,1,18426", "topic,1,21892", "topic,1,9819",
				"topic,1,64464", "topic,1,831315", "topic,1,872",
				"topic,1,28876", "topic,1,385552", "topic,1,50058",
				"topic,1,1825", "topic,1,44202", "topic,1,97276",
				"topic,1,574864", "topic,1,89963", "topic,1,1166",
				"topic,1,26240", "topic,1,24899", "topic,1,507510",
				"topic,1,1403", "topic,1,13888", "topic,1,137858",
				"topic,1,297275", "topic,1,6143", "topic,1,3666",
				"topic,1,271382", "topic,1,65430", "topic,1,6712",
				"topic,1,5958", "topic,1,276", "topic,1,1733", "topic,1,3636",
				"topic,1,16548", "topic,1,220", "topic,1,57479",
				"topic,1,59656", "topic,1,7643", "topic,1,84005",
				"topic,1,51686", "topic,1,146513", "topic,1,96951",
				"topic,1,546282", "topic,1,38136", "topic,1,592376",
				"topic,1,41352", "topic,1,115490", "topic,1,8530",
				"topic,1,425257", "topic,1,85330", "topic,1,2883",
				"topic,1,52323", "topic,1,884", "topic,1,609514",
				"topic,1,611727", "topic,1,791", "topic,1,15657",
				"topic,1,2168", "topic,1,46265", "topic,1,84858",
				"topic,1,7034", "topic,1,141122", "topic,1,869240",
				"topic,1,522", "topic,1,53610", "topic,1,329580",
				"topic,1,80823", "topic,1,886056", "topic,1,76455",
				"topic,1,51815", "topic,1,964705", "topic,1,22196",
				"topic,1,96260", "topic,1,2214", "topic,1,2019",
				"topic,1,238741", "topic,1,3667", "topic,1,38896",
				"topic,1,68740", "topic,1,27989", "topic,1,51979",
				"topic,1,50240", "topic,1,702852", "topic,1,4761",
				"topic,1,764954", "topic,1,3752", "topic,1,6994",
				"topic,1,75391", "topic,1,14936", "topic,1,884611",
				"topic,1,974505", "topic,1,751281", "topic,1,5207",
				"topic,1,117814", "topic,1,7448", "topic,1,4579",
				"topic,1,9565", "topic,1,787352", "topic,1,9168",
				"topic,1,566475", "topic,1,966191", "topic,1,331707",
				"topic,1,82472", "topic,1,547821", "topic,1,21374",
				"topic,1,20557", "topic,1,1180", "topic,1,672712",
				"topic,1,7740", "topic,1,944495", "topic,1,82808",
				"topic,1,333588", "topic,1,931061", "topic,1,6601",
				"topic,1,40712", "topic,1,7900", "topic,1,43629",
				"topic,1,102242", "topic,1,2454", "topic,1,300766",
				"topic,1,428", "topic,1,108792", "topic,1,27242",
				"topic,1,904044", "topic,1,838646", "topic,1,526090",
				"topic,1,401723", "topic,1,2427", "topic,1,6029",
				"topic,1,6063", "topic,1,9384", "topic,1,12042", "topic,1,325",
				"topic,1,5906", "topic,1,871014", "topic,1,215219",
				"topic,1,4603", "topic,1,50937", "topic,1,76991", "topic,1,72",
				"topic,1,1493", "topic,1,469640", "topic,1,2540",
				"topic,1,1689", "topic,1,294233", "topic,1,996599",
				"topic,1,9769", "topic,1,4332", "topic,1,72802",
				"topic,1,333314", "topic,1,262306", "topic,1,94173",
				"topic,1,56444", "topic,1,676040", "topic,1,6181",
				"topic,1,7267", "topic,1,73987", "topic,1,645241",
				"topic,1,428519", "topic,1,48944", "topic,1,150595",
				"topic,1,747689", "topic,1,2512", "topic,1,867040",
				"topic,1,622050", "topic,1,47823", "topic,1,2271",
				"topic,1,798392", "topic,1,6904", "topic,1,48656",
				"topic,1,816753", "topic,1,66082", "topic,1,618968",
				"topic,1,20597", "topic,1,172", "topic,1,859328",
				"topic,1,2869", "topic,1,7314", "topic,1,42492",
				"topic,1,39388", "topic,1,476155", "topic,1,8648",
				"topic,1,6563", "topic,1,756910", "topic,1,91173",
				"topic,1,5680", "topic,1,6915", "topic,1,93085",
				"topic,1,18918", "topic,1,7255", "topic,1,30542",
				"topic,1,196535", "topic,1,537147", "topic,1,385017",
				"topic,1,514371", "topic,1,47582", "topic,1,24244",
				"topic,1,70431", "topic,1,446942", "topic,1,73202",
				"topic,1,5390", "topic,1,194306", "topic,1,871969",
				"topic,1,608963", "topic,1,572409", "topic,1,522212",
				"topic,1,434", "topic,1,247460", "topic,1,436774",
				"topic,1,6438", "topic,1,76116", "topic,1,108601",
				"topic,1,4763", "topic,1,81884", "topic,1,5621",
				"topic,1,105398", "topic,1,31908", "topic,1,271047",
				"topic,1,5710", "topic,1,5202", "topic,1,279834",
				"topic,1,7011", "topic,1,5107", "topic,1,87129",
				"topic,1,6089", "topic,1,199988", "topic,1,74271",
				"topic,1,14847", "topic,1,99051", "topic,1,58974",
				"topic,1,3694", "topic,1,890897", "topic,1,3871",
				"topic,1,11866", "topic,1,77377", "topic,1,4320",
				"topic,1,8326", "topic,1,722734", "topic,1,927055",
				"topic,1,986131", "topic,1,1829", "topic,1,4404",
				"topic,1,66330", "topic,1,950227", "topic,1,50364",
				"topic,1,45934", "topic,1,232379", "topic,1,3538",
				"topic,1,61111", "topic,1,254952", "topic,1,61189",
				"topic,1,97764", "topic,1,70538", "topic,1,453",
				"topic,1,5365", "topic,1,61777", "topic,1,7552",
				"topic,1,41332", "topic,1,403640", "topic,1,86020",
				"topic,1,3539", "topic,1,4525", "topic,1,6252",
				"topic,1,41495", "topic,1,18184", "topic,1,9572",
				"topic,1,983394", "topic,1,874165", "topic,1,751281",
				"topic,1,543", "topic,1,730403", "topic,1,7230",
				"topic,1,24750", "topic,1,33748", "topic,1,2711",
				"topic,1,7433", "topic,1,452721", "topic,1,10604",
				"topic,1,12113", "topic,1,75925", "topic,1,74475",
				"topic,1,21075", "topic,1,44234", "topic,1,9346",
				"topic,1,74113", "topic,1,9288", "topic,1,88888",
				"topic,1,993517", "topic,1,56674", "topic,1,3682",
				"topic,1,50504", "topic,1,33278", "topic,1,984400",
				"topic,1,430147", "topic,1,9010", "topic,1,974493",
				"topic,1,13928", "topic,1,4719", "topic,1,867535",
				"topic,1,9831", "topic,1,72228", "topic,1,1544", "topic,1,957",
				"topic,1,239087", "topic,1,66412", "topic,1,36830",
				"topic,1,68419", "topic,1,9748", "topic,1,9141",
				"topic,1,37437", "topic,1,8872", "topic,1,939640",
				"topic,1,600776", "topic,1,293993", "topic,1,412505",
				"topic,1,33291", "topic,1,863861", "topic,1,664371",
				"topic,1,6069", "topic,1,795418", "topic,1,83847",
				"topic,1,770415", "topic,1,7558", "topic,1,106644",
				"topic,1,86308", "topic,1,7136", "topic,1,98246",
				"topic,1,141599", "topic,1,86860", "topic,1,9053",
				"topic,1,1501", "topic,1,33902", "topic,1,9127",
				"topic,1,43195", "topic,1,72797", "topic,1,39734",
				"topic,1,9101", "topic,1,378423", "topic,1,936675",
				"topic,1,8437", "topic,1,3710", "topic,1,620340",
				"topic,1,60165", "topic,1,2741", "topic,1,619384",
				"topic,1,4123", "topic,1,87587", "topic,1,7639",
				"topic,1,601715", "topic,1,8977", "topic,1,640360",
				"topic,1,31658", "topic,1,10019", "topic,1,560003",
				"topic,1,24631", "topic,1,75007", "topic,1,364851",
				"topic,1,667732", "topic,1,342122", "topic,1,688770",
				"topic,1,298718", "topic,1,66050", "topic,1,12667",
				"topic,1,40864", "topic,1,79117", "topic,1,70296",
				"topic,1,9043", "topic,1,7454", "topic,1,228015",
				"topic,1,67228", "topic,1,99505", "topic,1,7781",
				"topic,1,510207", "topic,1,94655", "topic,1,82136",
				"topic,1,58071", "topic,1,32863", "topic,1,6034",
				"topic,1,63778", "topic,1,76753", "topic,1,7227",
				"topic,1,47630", "topic,1,75509", "topic,1,610186",
				"topic,1,755286", "topic,1,1658", "topic,1,757696",
				"topic,1,20137", "topic,1,20985", "topic,1,740190",
				"topic,1,96941", "topic,1,30613", "topic,1,957557",
				"topic,1,108570", "topic,1,7622", "topic,1,69465",
				"topic,1,83223", "topic,1,72277", "topic,1,582345",
				"topic,1,5155", "topic,1,548824", "topic,1,527759",
				"topic,1,13350", "topic,1,80790", "topic,1,51801",
				"topic,1,88669", "topic,1,495168", "topic,1,45560",
				"topic,1,679054", "topic,1,821986", "topic,1,14347",
				"topic,1,8237", "topic,1,566942", "topic,1,61842",
				"topic,1,125507", "topic,1,78420", "topic,1,29175",
				"topic,1,35962", "topic,1,214763", "topic,1,22839",
				"topic,1,59256", "topic,1,38658", "topic,1,752012",
				"topic,1,1189", "topic,1,4388", "topic,1,671412",
				"topic,1,619452", "topic,1,8943", "topic,1,1444",
				"topic,1,569", "topic,1,641246", "topic,1,5086",
				"topic,1,55247", "topic,1,65712", "topic,1,166848",
				"topic,1,56496", "topic,1,488035", "topic,1,67413",
				"topic,1,63425", "topic,1,303445", "topic,1,238845",
				"topic,1,9928", "topic,1,339929", "topic,1,46497",
				"topic,1,95181", "topic,1,5362", "topic,1,6028",
				"topic,1,4101", "topic,1,53199", "topic,1,9807",
				"topic,1,298543", "topic,1,11676", "topic,1,24364",
				"topic,1,3506", "topic,1,963274", "topic,1,12411",
				"topic,1,88425", "topic,1,195260", "topic,1,7628",
				"topic,1,97381", "topic,1,23447", "topic,1,3265",
				"topic,1,8819", "topic,1,20253", "topic,1,17257",
				"topic,1,4153", "topic,1,3000", "topic,1,7938",
				"topic,1,454315", "topic,1,4894", "topic,1,9614",
				"topic,1,433846", "topic,1,75407", "topic,1,1466",
				"topic,1,26916", "topic,1,3757", "topic,1,18397",
				"topic,1,97028", "topic,1,52465", "topic,1,2554",
				"topic,1,470179", "topic,1,121739", "topic,1,342419",
				"topic,1,366180", "topic,1,34524", "topic,1,21093",
				"topic,1,178359", "topic,1,456866", "topic,1,227226",
				"topic,1,61209", "topic,1,22928", "topic,1,46319",
				"topic,1,453864", "topic,1,6594", "topic,1,875598",
				"topic,1,83992", "topic,1,5242", "topic,1,331722",
				"topic,1,6911", "topic,1,6812", "topic,1,932916",
				"topic,1,48549", "topic,1,40805", "topic,1,317150",
				"topic,1,17306", "topic,1,34558", "topic,1,96733",
				"topic,1,157651", "topic,1,45454", "topic,1,2104",
				"topic,1,63370", "topic,1,8966", "topic,1,24335",
				"topic,1,2536", "topic,1,92101", "topic,1,169990",
				"topic,1,70200", "topic,1,100882", "topic,1,5645",
				"topic,1,57659", "topic,1,856662", "topic,1,23560",
				"topic,1,1571", "topic,1,99092", "topic,1,44167",
				"topic,1,842360", "topic,1,5415", "topic,1,1204",
				"topic,1,18665", "topic,1,4003", "topic,1,384536",
				"topic,1,4767", "topic,1,289616", "topic,1,3251",
				"topic,1,57174", "topic,1,56244", "topic,1,786461",
				"topic,1,854563", "topic,1,3437", "topic,1,456545",
				"topic,1,8305", "topic,1,7175", "topic,1,112214",
				"topic,1,7921", "topic,1,874976", "topic,1,7570",
				"topic,1,64408", "topic,1,138722", "topic,1,36496",
				"topic,1,4009", "topic,1,742953", "topic,1,234",
				"topic,1,70150", "topic,1,3917", "topic,1,6689",
				"topic,1,91889", "topic,1,44740", "topic,1,31637",
				"topic,1,229157", "topic,1,42617", "topic,1,3923",
				"topic,1,6256", "topic,1,462566", "topic,1,5877",
				"topic,1,2007", "topic,1,7340", "topic,1,934601",
				"topic,1,856543", "topic,1,853", "topic,1,501945",
				"topic,1,3820", "topic,1,707105", "topic,1,2508",
				"topic,1,4049", "topic,1,16945", "topic,1,383883",
				"topic,1,980569", "topic,1,1812", "topic,1,208126",
				"topic,1,212327", "topic,1,958677", "topic,1,1404",
				"topic,1,454407", "topic,1,35349", "topic,1,75",
				"topic,1,81223", "topic,1,8434", "topic,1,390740",
				"topic,1,5912", "topic,1,780310", "topic,1,445521",
				"topic,1,72345", "topic,1,800875", "topic,1,23697",
				"topic,1,708602", "topic,1,63564", "topic,1,76676",
				"topic,1,300076", "topic,1,53838", "topic,1,3092",
				"topic,1,51899", "topic,1,834056", "topic,1,1043",
				"topic,1,8227", "topic,1,86682", "topic,1,46174",
				"topic,1,565339", "topic,1,37658", "topic,1,5577",
				"topic,1,900596", "topic,1,11376", "topic,1,8521",
				"topic,1,556218", "topic,1,67307", "topic,1,831283",
				"topic,1,583966", "topic,1,49172", "topic,1,93777",
				"topic,1,7109", "topic,1,883813", "topic,1,574534",
				"topic,1,829004", "topic,1,6154", "topic,1,18472",
				"topic,1,4849", "topic,1,320", "topic,1,431342",
				"topic,1,87743", "topic,1,12016", "topic,1,2754",
				"topic,1,9116", "topic,1,32994", "topic,1,8903",
				"topic,1,9449", "topic,1,468830", "topic,1,93759",
				"topic,1,3456", "topic,1,8000", "topic,1,2059",
				"topic,1,904976", "topic,1,595345", "topic,1,897500",
				"topic,1,373983", "topic,1,2314", "topic,1,56638",
				"topic,1,541575", "topic,1,86397", "topic,1,216343",
				"topic,1,342071", "topic,1,3262", "topic,1,934177",
				"topic,1,1727", "topic,1,154", "topic,1,269775", "topic,1,207",
				"topic,1,16936", "topic,1,163421", "topic,1,16874",
				"topic,1,104761", "topic,1,578506", "topic,1,737524",
				"topic,1,7035", "topic,1,655377", "topic,1,1015",
				"topic,1,31695", "topic,1,547245", "topic,1,503405",
				"topic,1,460306", "topic,1,26513", "topic,1,12890",
				"topic,1,475935", "topic,1,851265", "topic,1,187090",
				"topic,1,973547", "topic,1,967789", "topic,1,56953",
				"topic,1,77254", "topic,1,283597", "topic,1,84240",
				"topic,1,904032", "topic,1,4760", "topic,1,840498",
				"topic,1,453557", "topic,1,4563", "topic,1,2178",
				"topic,1,36486", "topic,1,5925", "topic,1,75130",
				"topic,1,748553", "topic,1,36127", "topic,1,8703",
				"topic,1,652106", "topic,1,5161", "topic,1,27000",
				"topic,1,62567", "topic,1,64495", "topic,1,3350",
				"topic,1,791320", "topic,1,4508", "topic,1,6034",
				"topic,1,82164", "topic,1,1995", "topic,1,430179",
				"topic,1,68091", "topic,1,67113", "topic,1,90581",
				"topic,1,5043", "topic,1,8326", "topic,1,620662",
				"topic,1,512418", "topic,1,404145", "topic,1,7431",
				"topic,1,414459", "topic,1,6411", "topic,1,43779",
				"topic,1,7093", "topic,1,314168", "topic,1,646183",
				"topic,1,927968", "topic,1,754706", "topic,1,111824",
				"topic,1,846098", "topic,1,30343", "topic,1,3663",
				"topic,1,366910", "topic,1,79309", "topic,1,832255",
				"topic,1,210042", "topic,1,6135", "topic,1,86561",
				"topic,1,32760", "topic,1,780889", "topic,1,2535",
				"topic,1,9165", "topic,1,343939", "topic,1,69718",
				"topic,1,142683", "topic,1,846235", "topic,1,25747",
				"topic,1,116478", "topic,1,806738", "topic,1,191",
				"topic,1,700196", "topic,1,702579", "topic,1,64086",
				"topic,1,434477", "topic,1,210000", "topic,1,26440",
				"topic,1,97472", "topic,1,54825", "topic,1,30202",
				"topic,1,315021", "topic,1,87205", "topic,1,79498",
				"topic,1,499617", "topic,1,684", "topic,1,9646",
				"topic,1,1700", "topic,1,561841", "topic,1,640099",
				"topic,1,1503", "topic,1,2180", "topic,1,94222",
				"topic,1,3649", "topic,1,329002", "topic,1,749578",
				"topic,1,861493", "topic,1,930194", "topic,1,919163",
				"topic,1,980171", "topic,1,11054", "topic,1,7270",
				"topic,1,779882", "topic,1,950891", "topic,1,414894",
				"topic,1,9828", "topic,1,8901", "topic,1,708408",
				"topic,1,97111", "topic,1,50066", "topic,1,670649",
				"topic,1,814915", "topic,1,354", "topic,1,29000",
				"topic,1,5750", "topic,1,506037", "topic,1,7023",
				"topic,1,520677", "topic,1,296203", "topic,1,828841",
				"topic,1,3496", "topic,1,2272", "topic,1,994138",
				"topic,1,64487", "topic,1,862605", "topic,1,38431",
				"topic,1,844256", "topic,1,48505", "topic,1,3098",
				"topic,1,306572", "topic,1,30365", "topic,1,689651",
				"topic,1,12928", "topic,1,131306", "topic,1,222677",
				"topic,1,90224", "topic,1,436867", "topic,1,95555",
				"topic,1,7932", "topic,1,7319", "topic,1,25883",
				"topic,1,358956", "topic,1,75412", "topic,1,2295",
				"topic,1,58519", "topic,1,23525", "topic,1,55941",
				"topic,1,85964", "topic,1,42903", "topic,1,99035",
				"topic,1,6396", "topic,1,376059", "topic,1,83286",
				"topic,1,1577", "topic,1,806202", "topic,1,561820",
				"topic,1,60664", "topic,1,25419", "topic,1,95840",
				"topic,1,572", "topic,1,3278", "topic,1,77987",
				"topic,1,748252", "topic,1,2410", "topic,1,560",
				"topic,1,46682", "topic,1,8763", "topic,1,29850",
				"topic,1,427518", "topic,1,23910", "topic,1,8133",
				"topic,1,26424", "topic,1,99950", "topic,1,98939",
				"topic,1,9312", "topic,1,298547", "topic,1,59148",
				"topic,1,626025", "topic,1,54777", "topic,1,577084",
				"topic,1,950971", "topic,1,67974", "topic,1,88623",
				"topic,1,782833", "topic,1,59540", "topic,1,567521",
				"topic,1,37129", "topic,1,3227", "topic,1,261153",
				"topic,1,9492", "topic,1,1663", "topic,1,37080",
				"topic,1,41628", "topic,1,2227", "topic,1,4943",
				"topic,1,22778", "topic,1,113386", "topic,1,77109",
				"topic,1,16731", "topic,1,42627", "topic,1,3545",
				"topic,1,991953", "topic,1,228595", "topic,1,274608",
				"topic,1,54590", "topic,1,900391", "topic,1,25957",
				"topic,1,33243", "topic,1,88130", "topic,1,70431",
				"topic,1,55576", "topic,1,6848", "topic,1,377998",
				"topic,1,931534", "topic,1,250523", "topic,1,98284",
				"topic,1,912032", "topic,1,2718", "topic,1,4640",
				"topic,1,167812", "topic,1,43702", "topic,1,852620",
				"topic,1,11967", "topic,1,122750", "topic,1,409790",
				"topic,1,5705", "topic,1,32693", "topic,1,507700",
				"topic,1,1773", "topic,1,82040", "topic,1,992630",
				"topic,1,73256", "topic,1,833049", "topic,1,10472",
				"topic,1,435", "topic,1,41943", "topic,1,47208",
				"topic,1,46873", "topic,1,554047", "topic,1,6039",
				"topic,1,1814", "topic,1,8980", "topic,1,473", "topic,1,16781",
				"topic,1,4341", "topic,1,51695", "topic,1,902145",
				"topic,1,728902", "topic,1,644415", "topic,1,540843",
				"topic,1,2746", "topic,1,18106", "topic,1,45797",
				"topic,1,697539", "topic,1,185", "topic,1,9192",
				"topic,1,172442", "topic,1,18392", "topic,1,67125",
				"topic,1,29646", "topic,1,3555", "topic,1,2817",
				"topic,1,32234", "topic,1,4646", "topic,1,68022",
				"topic,1,54758", "topic,1,29304", "topic,1,81704",
				"topic,1,847", "topic,1,18773", "topic,1,645763",
				"topic,1,6458", "topic,1,832748", "topic,1,935693",
				"topic,1,77589", "topic,1,4895", "topic,1,6458",
				"topic,1,172590", "topic,1,86331", "topic,1,90998",
				"topic,1,6733", "topic,1,86887", "topic,1,416373",
				"topic,1,324281", "topic,1,955928", "topic,1,1889",
				"topic,1,817482", "topic,1,323948", "topic,1,306635",
				"topic,1,48628", "topic,1,2720", "topic,1,125263",
				"topic,1,259095", "topic,1,746750", "topic,1,974408",
				"topic,1,41802", "topic,1,205361", "topic,1,5634",
				"topic,1,54017", "topic,1,67502", "topic,1,9845",
				"topic,1,27954", "topic,1,133923", "topic,1,556525",
				"topic,1,199988", "topic,1,67302", "topic,1,5639",
				"topic,1,865502", "topic,1,534369", "topic,1,6339",
				"topic,1,4570", "topic,1,778260", "topic,1,2416",
				"topic,1,180902", "topic,1,278675", "topic,1,6515",
				"topic,1,413129", "topic,1,466534", "topic,1,794454",
				"topic,1,978082", "topic,1,8937", "topic,1,4171",
				"topic,1,997696", "topic,1,431308", "topic,1,379156",
				"topic,1,55413", "topic,1,604281", "topic,1,59291",
				"topic,1,27224", "topic,1,423437", "topic,1,499126",
				"topic,1,3254", "topic,1,50931", "topic,1,54926",
				"topic,1,66953", "topic,1,5849", "topic,1,38603",
				"topic,1,82741", "topic,1,77636", "topic,1,213676",
				"topic,1,33995", "topic,1,318954", "topic,1,75807",
				"topic,1,28645", "topic,1,68526", "topic,1,44150",
				"topic,1,100", "topic,1,59847", "topic,1,420", "topic,1,4501",
				"topic,1,10267", "topic,1,2276", "topic,1,2094",
				"topic,1,760066", "topic,1,8281", "topic,1,531391",
				"topic,1,52803", "topic,1,68155", "topic,1,7557",
				"topic,1,121760", "topic,1,9422", "topic,1,763049",
				"topic,1,117295", "topic,1,725862", "topic,1,160999",
				"topic,1,166611", "topic,1,9254", "topic,1,83862",
				"topic,1,45662", "topic,1,34787", "topic,1,7527",
				"topic,1,79328", "topic,1,460221", "topic,1,3338",
				"topic,1,6616", "topic,1,2518", "topic,1,6672",
				"topic,1,969532", "topic,1,2707", "topic,1,36582",
				"topic,1,152296", "topic,1,7035", "topic,1,787321",
				"topic,1,6047", "topic,1,282717", "topic,1,4846",
				"topic,1,41869", "topic,1,6294", "topic,1,70069",
				"topic,1,2174", "topic,1,70850", "topic,1,27202",
				"topic,1,10060", "topic,1,4838", "topic,1,8562",
				"topic,1,9487", "topic,1,6419", "topic,1,431646",
				"topic,1,3077", "topic,1,1881", "topic,1,927789",
				"topic,1,860068", "topic,1,13980", "topic,1,682970",
				"topic,1,60841", "topic,1,61461", "topic,1,27930",
				"topic,1,75063", "topic,1,38291", "topic,1,6095",
				"topic,1,2242", "topic,1,64920", "topic,1,4443",
				"topic,1,164382", "topic,1,45566", "topic,1,1560",
				"topic,1,871896", "topic,1,38531", "topic,1,1882",
				"topic,1,385719", "topic,1,8023", "topic,1,5195",
				"topic,1,2734", "topic,1,72991", "topic,1,13148",
				"topic,1,8273", "topic,1,109023", "topic,1,9611",
				"topic,1,677236", "topic,1,8626", "topic,1,8224",
				"topic,1,96044", "topic,1,4206", "topic,1,190189",
				"topic,1,23899", "topic,1,1682", "topic,1,167009",
				"topic,1,52108", "topic,1,89547", "topic,1,598910",
				"topic,1,40580", "topic,1,462068", "topic,1,75262",
				"topic,1,15674", "topic,1,269618", "topic,1,239556",
				"topic,1,77891", "topic,1,40865", "topic,1,49297",
				"topic,1,1953", "topic,1,8097", "topic,1,23568",
				"topic,1,3215", "topic,1,893648", "topic,1,188687",
				"topic,1,711490", "topic,1,1910", "topic,1,79236",
				"topic,1,82310", "topic,1,8688", "topic,1,96064",
				"topic,1,8341", "topic,1,722509", "topic,1,463035",
				"topic,1,16207", "topic,1,4767", "topic,1,34850",
				"topic,1,215377", "topic,1,85792", "topic,1,64206",
				"topic,1,995986", "topic,1,50854", "topic,1,36392",
				"topic,1,28725", "topic,1,5410", "topic,1,59156",
				"topic,1,18339", "topic,1,9810", "topic,1,84444",
				"topic,1,95753", "topic,1,8620", "topic,1,6137",
				"topic,1,11677", "topic,1,6301", "topic,1,81944",
				"topic,1,3980", "topic,1,333235", "topic,1,35272",
				"topic,1,8435", "topic,1,2429", "topic,1,429098",
				"topic,1,2214", "topic,1,8479", "topic,1,721359",
				"topic,1,99603", "topic,1,9133", "topic,1,599481",
				"topic,1,770115", "topic,1,485063", "topic,1,84179",
				"topic,1,242124", "topic,1,40576", "topic,1,761582",
				"topic,1,575", "topic,1,9031", "topic,1,85188",
				"topic,1,166978", "topic,1,41436", "topic,1,32096",
				"topic,1,47041", "topic,1,6480", "topic,1,3077",
				"topic,1,1328", "topic,1,393172", "topic,1,593308",
				"topic,1,185614", "topic,1,69824", "topic,1,34919",
				"topic,1,649621", "topic,1,70173", "topic,1,82414",
				"topic,1,903703", "topic,1,88824", "topic,1,19095",
				"topic,1,9834", "topic,1,3968", "topic,1,92701",
				"topic,1,2458", "topic,1,86293", "topic,1,47974",
				"topic,1,26546", "topic,1,873333", "topic,1,72539",
				"topic,1,515581", "topic,1,72035", "topic,1,6188",
				"topic,1,929037", "topic,1,210401", "topic,1,87241",
				"topic,1,514542", "topic,1,11289", "topic,1,29198",
				"topic,1,33695", "topic,1,44987", "topic,1,645052",
				"topic,1,56817", "topic,1,4396", "topic,1,93104",
				"topic,1,164562", "topic,1,29215", "topic,1,2685",
				"topic,1,9873", "topic,1,687321", "topic,1,6716",
				"topic,1,877630", "topic,1,438794", "topic,1,33576",
				"topic,1,2666", "topic,1,630500", "topic,1,4871",
				"topic,1,80420", "topic,1,16787", "topic,1,393",
				"topic,1,58311", "topic,1,685838", "topic,1,698405",
				"topic,1,339772", "topic,1,254006", "topic,1,336466",
				"topic,1,799754", "topic,1,629653", "topic,1,70863",
				"topic,1,4095", "topic,1,98225", "topic,1,1042",
				"topic,1,9002", "topic,1,7094", "topic,1,1499",
				"topic,1,96550", "topic,1,531141", "topic,1,7753",
				"topic,1,919206", "topic,1,61787", "topic,1,8742",
				"topic,1,2478", "topic,1,318720", "topic,1,3256",
				"topic,1,238402", "topic,1,4710", "topic,1,3080",
				"topic,1,1196", "topic,1,428455", "topic,1,62383",
				"topic,1,43401", "topic,1,24968", "topic,1,744310",
				"topic,1,61683", "topic,1,9426", "topic,1,606745",
				"topic,1,230607", "topic,1,90711", "topic,1,94349",
				"topic,1,10280", "topic,1,8695", "topic,1,5354",
				"topic,1,735975", "topic,1,8185", "topic,1,81175",
				"topic,1,5471", "topic,1,4622", "topic,1,4917",
				"topic,1,667886", "topic,1,80728", "topic,1,7411",
				"topic,1,9433", "topic,1,1834", "topic,1,391766",
				"topic,1,70133", "topic,1,31465", "topic,1,39121",
				"topic,1,58489", "topic,1,2030", "topic,1,231795",
				"topic,1,6862", "topic,1,1657", "topic,1,3108",
				"topic,1,407207", "topic,1,6705", "topic,1,60337",
				"topic,1,35773", "topic,1,31589", "topic,1,3188",
				"topic,1,87664", "topic,1,8556", "topic,1,1871",
				"topic,1,655214", "topic,1,74314", "topic,1,44096",
				"topic,1,2894", "topic,1,99548" };

		// String[] inputs = null;

		List<String> inputSet = Lists.newArrayList();
		/*
		 * InputStream inputStream = Driver.class
		 * .getResourceAsStream("/allocation.report"); BufferedReader br = new
		 * BufferedReader(new InputStreamReader( inputStream));
		 * 
		 * String line; while ((line = br.readLine()) != null) { String newLine
		 * = line.replaceAll("/", ","); inputSet.add(newLine); } br.close();
		 */

		String[] newInput = new String[inputSet.size()];
		Set<OptimTuple> jobs = getReduceDataSetsFromStringInput(inputs);
		List<Long> array = new ArrayList<Long>();
		for (OptimTuple job : jobs) {
			array.add(job.getJobSize());
		}
		System.out.print(array);
		System.out
				.println("***************************** PRIORITY QUEUE BASED IMPLEMENTATION ****************************");
		IJobLoadOptimizer iJobLoadOptimizer = JobLoadOptimizerFactory
				.getJobLoadOptimizerFactory(JobLoadOptimizerFactory.Optimizer.PRIORITY_QUEUE_BASED);
		List<Set<IInputJob>> optimizedLoadSets = iJobLoadOptimizer
				.getOptimizedLoadSets(jobs, numberOfProcessors);
		printResult(optimizedLoadSets);

		System.out.println();
		System.out
				.println("*********************************** AMBITIOUS IMPLEMENTATION **********************************");
		iJobLoadOptimizer = JobLoadOptimizerFactory
				.getJobLoadOptimizerFactory(JobLoadOptimizerFactory.Optimizer.AMBITIOUS);
		optimizedLoadSets = iJobLoadOptimizer.getOptimizedLoadSets(jobs,
				numberOfProcessors);
		printResult(optimizedLoadSets);

		// System.out.println();
		// System.out
		// .println("*********************************** First Fit IMPLEMENTATION **********************************");
		// iJobLoadOptimizer = JobLoadOptimizerFactory
		// .getJobLoadOptimizerFactory(JobLoadOptimizerFactory.Optimizer.FirstFitOptimizer);
		// optimizedLoadSets = iJobLoadOptimizer.getOptimizedLoadSets(jobs,
		// numberOfProcessors);
		// printResult(optimizedLoadSets);

	}

	private static void printResult(List<Set<IInputJob>> optimizedLoadSets) {
		List<BigInteger> list = Lists.newArrayList();
		int index = 0;
		for (Set<IInputJob> optSet : optimizedLoadSets) {
			BigInteger size = new BigInteger("0");
			for (IInputJob job : optSet) {
				size = size
						.add(new BigInteger(String.valueOf(job.getJobSize())));
			}
			list.add(size);
			System.out.println("Processor/Reducer :" + index + "  Job Set: "
					+ optSet);
			System.out.println("Total job size " + size);
			index++;
		}
		System.out.println("Standard Deviation : " + standardDeviation(list));
	}

	private static BigInteger standardDeviation(List<BigInteger> list) {
		System.out.println(list);
		BigInteger standradDeviation = new BigInteger("0");
		BigInteger sum = new BigInteger("0");
		for (BigInteger num : list) {
			sum = sum.add(num);
		}
		BigInteger average = sum.divide(new BigInteger(String.valueOf(list
				.size())));
		for (BigInteger num : list) {
			BigInteger diff = num.subtract(average);
			diff = diff.abs();
			BigInteger square = diff.multiply(diff);
			standradDeviation = standradDeviation.add(square);
		}
		return standradDeviation;
	}

	private static Set<OptimTuple> getReduceDataSetsFromStringInput(
			String[] inputs) {
		Set<OptimTuple> jobs = new HashSet<OptimTuple>();
		for (String input : inputs) {
			jobs.add(new OptimTuple(input));
		}
		return jobs;
	}
}
