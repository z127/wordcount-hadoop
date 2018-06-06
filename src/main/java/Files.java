import java.io.*;

public class Files {

    /**
     * 文件分割
     * @param src 源文件路径
     * @param fileSize 分割后每个文件的大小，单位是MB
     * @param dest 目标文件路径
     */
    public static void split(String src,int fileSize,String dest){

        if("".equals(src)||src==null||fileSize==0||"".equals(dest)||dest==null){
            System.out.println("分割失败");
        }

        File srcFile = new File(src);//源文件

        long srcSize = srcFile.length();//源文件的大小
        long destSize = 1024*1024*fileSize;//目标文件的大小（分割后每个文件的大小）

        int number = (int)(srcSize/destSize);
        number = srcSize%destSize==0?number:number+1;//分割后文件的数目

        String fileName = src.substring(src.lastIndexOf("\\"));//源文件名

        InputStream in = null;//输入字节流
        BufferedInputStream bis = null;//输入缓冲流
        byte[] bytes = new byte[1024*1024];//每次读取文件的大小为1MB
        int len = -1;//每次读取的长度值
        try {
            in = new FileInputStream(srcFile);
            bis = new BufferedInputStream(in);
            for(int i=0;i<number;i++){

                String destName = dest+File.separator+fileName+"-"+i+".dat";
                OutputStream out = new FileOutputStream(destName);
                BufferedOutputStream bos = new BufferedOutputStream(out);
                int count = 0;
                while((len = bis.read(bytes))!=-1){
                    bos.write(bytes, 0, len);//把字节数据写入目标文件中
                    count+=len;
                    if(count>=destSize){
                        break;
                    }
                }
                bos.flush();//刷新
                bos.close();
                out.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            //关闭流
            try {
                if(bis!=null)bis.close();
                if(in!=null)in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 文件合并
     * 注意：在拼接文件路劲时，一定不要忘记文件的跟路径，否则复制不成功
     * @param destPath 目标目录
     * @param srcPaths 源文件目录
     */
    public static void merge(String destPath,String ... srcPaths){
        if(destPath==null||"".equals(destPath)||srcPaths==null){
            System.out.println("合并失败");
        }
        for (String string : srcPaths) {
            if("".equals(string)||string==null)
                System.out.println("合并失败");
        }
        //合并后的文件名
        String name = srcPaths[0].substring(srcPaths[0].lastIndexOf("\\"));
        System.out.println(name);
        String destName = name.substring(0, name.lastIndexOf("_"));
        destPath = destPath+destName;//合并后的文件路径

        File destFile = new File(destPath);//合并后的文件
        OutputStream out = null;
        BufferedOutputStream bos = null;
        try {
            out = new FileOutputStream(destFile);
            bos = new BufferedOutputStream(out);
            for (String src : srcPaths) {
                File srcFile = new File(src);
                InputStream in = new FileInputStream(srcFile);
                BufferedInputStream bis = new BufferedInputStream(in);
                byte[] bytes = new byte[1024*1024];
                int len = -1;
                while((len = bis.read(bytes))!=-1){
                    bos.write(bytes, 0, len);
                }
                bis.close();
                in.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            //关闭流
            try {
                if(bos!=null)bos.close();
                if(out!=null)out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        /**
         * 分割测试
         */
    /*  String src = "H:\\split\\132.exe";//要分割的大文件
      int fileSize = 10;
      String dest = "H:\\split";//文件分割后保存的路径
     System.out.println("分割开始。。。");
      split(src, fileSize, dest);
    System.out.println("分割完成");*/

        /**
         * 合并测试
         */
        //合并后文件的保存路径
        String destPath = "H:\\split\\hebing";
        //要合并的文件路径
        String[] srcPaths = {
                "H:\\split\\blk_1074352123",
                "H:\\split\\blk_1074352124",
                "H:\\split\\blk_1074352125",
                "H:\\split\\blk_1074352126",
                "H:\\split\\blk_1074352127",
                "H:\\split\\blk_1074352128",
                };
        System.out.println("开始合并。。。");
        merge(destPath, srcPaths);
        System.out.println("合并结束");
    }

}  