/**
 * @file CodeCraft-2022.cpp
 * @author iammcy
 * @version 0.4
 * @date 2022-03-15
 * 
 * @copyright Copyright (c) 2022
 * 
 */
#include <bits/stdc++.h>
using namespace std;
#define Debug true

// 输入数据文件夹
#if Debug
const string DATA_DIR = "../data/";
#else
const string DATA_DIR = "/data/";
#endif

// 输出数据文件夹
#if Debug
const string OUTPUT_DIR = "../data/";
#else
const string OUTPUT_DIR = "/output/";
#endif

typedef long long ll;
struct degree
{
    int idx = 0;
    int num = 0;
};
struct used
{
    int idx = 0;
    ll u = 0;
};


bool cmp1(const degree & d1, const degree & d2){
    return d1.num < d2.num;
}
bool cmp2(const used & u1, const used & u2){
    return u1.u < u2.u;
}

class Scheduler
{
private:
    /* data */
    int T = 0, M = 0, N = 0, qos_constraint;
    vector<string> clients, sites;
    vector<vector<ll>> demands;
    vector<used> row_total_demands;
    vector<ll> site_used;
    vector<ll> site_bw;
    vector<priority_queue<ll>> site_cost;
    vector<vector<int>> qos;
    vector<degree> client_indegree;
    vector<degree> sites_outdegree;
    vector<vector<float>> weight;
    vector<used> acc_site_used;
    vector<int> acc_site_visited;
    vector<vector<ll>> ans;
    vector<string> res_list;
    
public:
    Scheduler(/* args */);
    ~Scheduler();
    void input();
    void output(int time);
    void scheduling();
    void output_all();
    void adjust_weight();
};

Scheduler::Scheduler(/* args */)
{
    /**
     * @brief 
     * @param data 
     * @return  
     */
}

Scheduler::~Scheduler()
{
}

void Scheduler::input()
{
    string line, field;
    // 1.读取demand.csv
    ifstream inFile1(DATA_DIR + "demand.csv", ios::in);
    if (!inFile1){
        cout<<"failed to open file"<<endl;
        exit(1);
    }

    // 1.1.读取表头
    getline(inFile1, line);
    line = line.substr(0, line.size()-1);
    istringstream sin(line);
    getline(sin, field, ',');
    while (getline(sin, field, ','))
    {
        clients.push_back(field);
        M++;
    }
    
    // 1.2.读取客户节点带宽需求
    while (getline(inFile1, line))
    {
        istringstream sin(line);
        vector<ll> tmp(M);
        int i = 0;
        getline(sin, field, ',');
        while (getline(sin, field, ','))
        {
            tmp[i] = atoll(field.c_str());
            i++;
        }
        demands.push_back(tmp);
        T++;
    }
    res_list = vector<string>(T);
    row_total_demands = vector<used>(T);
    for (int i = 0; i < T; i++){
        row_total_demands[i].idx = i;
        for (int j = 0; j < M; j++){
            row_total_demands[i].u -= demands[i][j];
        }
    }
    stable_sort(row_total_demands.begin(), row_total_demands.end(), cmp2);

    inFile1.close();    

    // 4.读取site_bandwidth.csv
    ifstream inFile4(DATA_DIR + "site_bandwidth.csv", ios::in);
    if (!inFile4){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile4, line);
    while(getline(inFile4, line)){
        istringstream sin(line);
        getline(sin, field, ',');
        sites.push_back(field);
        getline(sin, field, ',');
        ll bw = atoll(field.c_str());
        site_bw.push_back(bw);
        N++;
    }
    inFile4.close();

    // 2.读取config.ini
    ifstream inFile2(DATA_DIR + "config.ini", ios::in);
    if (!inFile2){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile2, line);
    getline(inFile2, line);
    istringstream conf_sin(line);
    getline(conf_sin, field, '=');
    getline(conf_sin, field, '=');
    qos_constraint = atoi(field.c_str());
    inFile2.close();

    // 3.读取qos.csv
    client_indegree = vector<degree>(M);
    for (int i=0; i<M; i++){
        client_indegree[i].idx = i;
    }
    sites_outdegree = vector<degree>(N);
    for (int i=0; i<N; i++){
        sites_outdegree[i].idx = i;
    }
    ifstream inFile3(DATA_DIR + "qos.csv", ios::in);
    if (!inFile3){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    getline(inFile3, line);
    int j = 0;
    while (getline(inFile3, line))
    {
        istringstream sin(line);
        vector<int> tmp(M, 0);
        int i = 0;
        getline(sin, field, ',');
        while (getline(sin, field, ','))
        {
            int x = atoi(field.c_str());
            if (x < qos_constraint){
                tmp[i] = 1;
                client_indegree[i].num++;
                sites_outdegree[j].num++;
            }
            i++;
        }
        qos.push_back(tmp);
        j++;
    }
    inFile3.close();

#if Debug
    cout<<"client degree: ";
    for (int i = 0; i < M; i++){
        cout<<client_indegree[i].num<<" ";
    }
    cout<<endl;

    cout<<"site degree: ";
    for (int i = 0; i < N; i++){
        cout<<sites_outdegree[i].num<<" ";
    }
    cout<<endl;
#endif

}

void Scheduler::adjust_weight()
{
    // 预处理客户节点的分配权重
    weight = vector<vector<float>>(N, vector<float>(M, 0));
    vector<float> sum(M, 0);
    for (int i=0; i<N; i++){
        float avg_bw = (float)(site_bw[i] - site_used[i]) / (float)sites_outdegree[i].num;
        for (int j=0; j<M; j++){
            if (qos[i][j] == 1)
            {
                weight[i][j] = avg_bw;
                sum[j] += avg_bw;
            }
        }
    }
    for (int i=0; i<N; i++){
        for (int j=0; j<M; j++){
            if (qos[i][j] == 1)
            {
                weight[i][j] = weight[i][j] / sum[j];
            }
        }
    }
}

void Scheduler::scheduling()
{
    // 预处理客户节点优先级
    // sort(client_indegree.begin(), client_indegree.end(), cmp1);

    // 预处理边缘节点优先级
    // sort(sites_outdegree.begin(), sites_outdegree.end(), cmp1);
    site_cost = vector<priority_queue<ll>>(N);
    acc_site_visited = vector<int>(N, 0);
    acc_site_used = vector<used>(N);
    for (int i = 0; i < N; i++)
        acc_site_used[i].idx = i;

    int site_used_num = 0, tag = 0;

    // 遍历所有时刻生成方案
    for (int i = 0; i < T; i++)
    {
        // 优先处理需求量大的时刻
        int time = row_total_demands[i].idx;

        // 初始化每个时刻的分配方案
        ans = vector<vector<ll>>(N, vector<ll>(M, 0));
        site_used = vector<ll>(N, 0);

        // 更新边缘节点分配顺序
        int tmp = 0;
        for (int j = 0; j < N; j++){
            if (acc_site_used[j].u > 0)
                tmp++;
        }
        if (i / (int)((float)T * 0.05 ) > tag && tmp > site_used_num){
            sort(acc_site_used.begin(), acc_site_used.end(), cmp2);
            tag = i / (int)((float)T * 0.05 );
            site_used_num = tmp;
        }

        // 存储已访问的边缘节点
        set<int> visited;

        for (int j = 0; j < M; j++)
        {
            int client_idx = client_indegree[j].idx;

            // 对该客户节点分配流量

            // 优先分配已访问的边缘节点
            for (auto idx : visited)
            {
                int sidx = acc_site_used[idx].idx;

                // 若边缘节点处在收费阶段，跳过优先分配策略
                if(acc_site_visited[sidx] >= (int)((float)T * 0.05 ))
                    continue;

                // 进入可分配边缘节点
                if (qos[sidx][client_idx] == 1 && (site_bw[sidx] - site_used[sidx] > 0))
                {
                    ll res = site_bw[sidx] - site_used[sidx];
                    
                    if (res < demands[time][client_idx])
                    {
                        site_used[sidx] = site_bw[sidx];
                        ans[sidx][client_idx] += res;
                        demands[time][client_idx] -= res;
                    }
                    else{
                        site_used[sidx] += demands[time][client_idx];
                        ans[sidx][client_idx] += demands[time][client_idx];
                        demands[time][client_idx] = 0;
                        break;
                    }
                }
            }

            // 以低于当前边缘节点成本的流量分配一遍
            for (int k = 0; k < N && demands[time][client_idx] > 0; k++)
            {
                int sidx = acc_site_used[k].idx;
                // 进入可分配边缘节点
                if (qos[sidx][client_idx] == 1 && (site_bw[sidx] - site_used[sidx] > 0) && (acc_site_visited[sidx] > (int)((float)T * 0.05 )) && (!site_cost.empty()) && (-site_cost[sidx].top() > site_used[sidx]))
                {
                    ll res = -site_cost[sidx].top() - site_used[sidx];
                    
                    if (res < demands[time][client_idx])
                    {
                        site_used[sidx] = -site_cost[sidx].top();
                        ans[sidx][client_idx] += res;
                        demands[time][client_idx] -= res;
                    }
                    else{
                        site_used[sidx] += demands[time][client_idx];
                        ans[sidx][client_idx] += demands[time][client_idx];
                        demands[time][client_idx] = 0;
                        break;
                    }
                }
            }
            

            // 遍历所有边缘节点
            for (int k = 0; k < N && demands[time][client_idx] > 0; k++)
            {   
                int sidx = acc_site_used[k].idx;
                // 进入可分配边缘节点
                if (qos[sidx][client_idx] == 1 && (site_bw[sidx] - site_used[sidx] > 0))
                {
                    ll res = site_bw[sidx] - site_used[sidx];
                    
                    if (res < demands[time][client_idx])
                    {
                        site_used[sidx] = site_bw[sidx];
                        ans[sidx][client_idx] += res;
                        demands[time][client_idx] -= res;
                    }
                    else{
                        site_used[sidx] += demands[time][client_idx];
                        ans[sidx][client_idx] += demands[time][client_idx];
                        demands[time][client_idx] = 0;
                    }
                    visited.insert(k);
                }
            }
            
        }

        for (int j = 0; j < N; j++)
        {
            acc_site_used[j].u += site_used[acc_site_used[j].idx];
        }
        

        for (int j = 0; j < N; j++){
            if (site_used[j] > 0){
                acc_site_visited[j]++;
                site_cost[j].push(-site_used[j]);
                if ((acc_site_visited[j] > (int)((float)T * 0.05 + 1))){
                    site_cost[j].pop();
                }
            }
        }

#if Debug
        // 输出边缘节点流量使用情况
        cout<<"Time "<<time<<": ";
        for (int j = 0; j < N; j++)
            cout<<site_used[j]<<" ";
        cout<<endl;
#endif

        // 检查客户节点是否分配完
        for (int j = 0; j < M; j++)
        {
            if (demands[time][j])
            {
                cout<<"分配不合理"<<endl;
                exit(1);
            }
        }
        
        this->output(time);
    }
    this->output_all();

#if Debug
    // 输出边缘节点有多少时刻被分配
    cout<<"site visited time: ";
    for (int i = 0; i < N; i++)
    {
        cout<<acc_site_visited[i]<<" ";
    }
    cout<<endl;

    // 输出边缘节点的近似成本以及总和
    ll cost = 0;
    cout<<"site cost: ";
    for (int i = 0; i < N; i++)
    {
        if (acc_site_visited[i] > (int)((float)T * 0.05)){
            cout<<-site_cost[i].top()<<" ";
            cost += -site_cost[i].top();
        }
        else{
            cout<<0<<" ";
        }
    }
    cout<<endl;
    cout<<"total cost: "<<cost<<endl;
     
#endif
}

void Scheduler::output(int time)
{
    string res = "";
    for (int j=0; j<M; j++){
        res += clients[j] + ":";
        bool tag = false;
        for (int k=0; k<N; k++){
            if (ans[k][j] > 0){
                res += "<" + sites[k] + "," + to_string(ans[k][j]) + ">,";
                tag = true;
            }
        }
        if (tag)
            res = res.substr(0, res.size()-1);
        if (time != T-1 || j != M-1)
        {
            res += "\n";
        }
    }
    res_list[time] = res;
}

void Scheduler::output_all()
{
    ofstream outFile(OUTPUT_DIR + "solution.txt", ios::app);
    if (!outFile){
        cout<<"failed to open file"<<endl;
        exit(1);
    }
    for (int i=0; i<T; i++)
        outFile<<res_list[i];
    outFile.close();
}


int main() {

#if Debug
    // 记录起始时间
    clock_t start, finish;
    start = clock();
#endif

    Scheduler scheduler;
    scheduler.input();
    scheduler.scheduling();

#if Debug
    // 记录结束时间
    finish = clock();
    cout<<(double)(finish - start) / CLOCKS_PER_SEC<<"s"<<endl;
#endif
	return 0;
}
