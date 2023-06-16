/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.hive.$internal.au.com.bytecode.opencsv.CSVWriter;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.TestAggregations;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.ITestAnnotation;
import org.testng.annotations.Test;
import org.testng.internal.ClassHelper;
import org.testng.internal.annotations.AnnotationHelper;
import org.testng.internal.annotations.IAnnotationFinder;
import org.testng.internal.annotations.JDK15AnnotationFinder;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Set;

public class test extends TestPrestoNativeAggregations
{
    static TestAggregations testClass;

    @BeforeClass
    protected void setQueryRunners() throws Exception {
        QueryRunner queryRunner = super.getQueryRunner();
        ExpectedQueryRunner expectedQueryRunner = super.getExpectedQueryRunner();

        testClass = new TestAggregations();
        testClass.setQueryRunner(queryRunner);
        testClass.setExpectedQueryRunner(expectedQueryRunner);
    }

    protected void restartQueryRunner() {
        try {
            setQueryRunners();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAggregations() {
        IAnnotationFinder finder = new JDK15AnnotationFinder(new IAnnotationTransformer() {
            @Override
            public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
                IAnnotationTransformer.super.transform(annotation, testClass, testConstructor, testMethod);
            }
        });

        Set<Method> allMethods = ClassHelper.getAvailableMethods(testClass.getClass());
        File file = new File("/Users/pramod/Desktop/testAggregations.csv");
        CSVWriter writer = null;

        try {
            FileWriter outputfile = new FileWriter(file);
            writer = new CSVWriter(outputfile);
            String[] header = {"Test", "Exception", "Message", "Stack Trace"};
            writer.writeNext(header);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        for (Method eachMethod : allMethods) {
            ITestAnnotation value = AnnotationHelper.findTest(finder, eachMethod);
            Object obj = testClass;
            if (value != null) {
                System.out.println("TEST NAME ============> " + eachMethod.getName());
                eachMethod.setAccessible(true);

                try {
                    setQueryRunners();
                } catch (Exception e1) {
                    throw new RuntimeException(e1);
                }

                try {
                    eachMethod.invoke(obj, null);
                } catch (Exception e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    String s1 = eachMethod.getName();
                    String s2 = (e.toString() == null) ? "" : e.toString();
                    String s3 = (e.getMessage() == null) ? "" : e.getMessage();
                    String s5 = (sw.toString() == null) ? "" : sw.toString();

                    // Trim the stack trace.
                    String[] lines = s5.split(System.getProperty("line.separator"));
                    for (int i = 0; i < lines.length; i++) {
                        lines[i] = lines[i].trim();
                        if (lines[i].startsWith("at com.") || lines[i].startsWith("at org.") || lines[i].startsWith("at sun.") || lines[i].startsWith("at java.")) {
                            lines[i] = "";
                        }
                    }
                    StringBuilder sb = new StringBuilder("");
                    for (String s:lines) {
                        if (!s.equals("")) {
                            sb.append(s).append(System.getProperty("line.separator"));
                        }
                    }
                    String s4 = sb.toString();

                    String[] sd = {s1, s2, s3, s4};
                    writer.writeNext(sd);
                    restartQueryRunner();
                }
            }
        }

        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}