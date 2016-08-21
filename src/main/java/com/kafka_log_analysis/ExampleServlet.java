package com.kafka_log_analysis;
 
import java.io.IOException;  
   
import javax.servlet.ServletException;  
import javax.servlet.annotation.WebServlet;  
import javax.servlet.http.HttpServlet;  
import javax.servlet.http.HttpServletRequest;  
import javax.servlet.http.HttpServletResponse;  
   
/** 
 * 
 * Servlet3.0支持使用注解配置Servlet。我们只需在Servlet对应的类上使用@WebServlet进行标注， 
 * 我们就可以访问到该Servlet了，而不需要再在web.xml文件中进行配置。@WebServlet的urlPatterns 
 * 和value属性都可以用来表示Servlet的部署路径，它们都是对应的一个数组。 
 */  
@WebServlet(name="example", urlPatterns="/servlet/e1")  
public class ExampleServlet extends HttpServlet {  
   
   private static final long serialVersionUID = 1L;  
   
   @Override  
   protected void doGet(HttpServletRequest request,  
         HttpServletResponse response) throws ServletException, IOException {  
      this.doPost(request, response);  
   }  
   
   @Override  
   protected void doPost(HttpServletRequest request,  
         HttpServletResponse response) throws ServletException, IOException {  
      response.getWriter().write("Hello User.");  
   }  
   
}  