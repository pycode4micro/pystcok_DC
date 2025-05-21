# 股票数据采集系统

这是一个用于采集股票数据的系统，支持同步和异步数据采集，具有高效的数据处理和数据库存储功能。系统严格遵循robots协议，确保对目标网站的友好访问。

## 功能特点

- 从东方财富网获取股票数据
- 支持同步和异步数据采集
- 使用内存缓存优化数据获取
- 将数据存储到MySQL数据库
- 支持批量数据插入
- 使用连接池优化数据库连接
- 优化的数据类型处理
- 支持环境变量配置
- 完善的错误处理和重试机制
- 严格遵循robots协议

## 环境要求

- Python 3.7+
- MySQL 5.7+

## 安装步骤

1. 克隆项目到本地：
```bash
git clone <repository_url>
cd pystock_DC
```

2. 安装依赖包：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
   - 复制 `.env.example` 文件为 `.env`
   - 修改 `.env` 文件中的配置信息

4. 初始化数据库：
```bash
python -m stock_simple.init_database
```

5. 运行数据采集：
   - 同步模式：
   ```bash
   python -m stock_simple.stock_simple
   ```
   - 异步模式：
   ```bash
   python -m stock_simple.stock_async
   ```

## 配置说明

在 `.env`